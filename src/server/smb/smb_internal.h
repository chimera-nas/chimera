// SPDX-FileCopyrightText: 2025 Ben Jarvis
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <stdint.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <utlist.h>
#include <gssapi/gssapi_ntlmssp.h>
#include "evpl/evpl.h"
#include "common/evpl_iovec_cursor.h"
#include "common/logging.h"
#include "common/misc.h"
#include "smb2.h"
#include "smb_attr.h"
#include "smb_session.h"
#include "smb_string.h"

#define SMB2_MAX_DIALECTS           16
#define SMB2_MAX_NEGOTIATE_CONTEXTS 16

#define chimera_smb_debug(...) chimera_debug("smb", \
                                             __FILE__, \
                                             __LINE__, \
                                             __VA_ARGS__)
#define chimera_smb_info(...)  chimera_info("smb", \
                                            __FILE__, \
                                            __LINE__, \
                                            __VA_ARGS__)
#define chimera_smb_error(...) chimera_error("smb", \
                                             __FILE__, \
                                             __LINE__, \
                                             __VA_ARGS__)
#define chimera_smb_fatal(...) chimera_fatal("smb", \
                                             __FILE__, \
                                             __LINE__, \
                                             __VA_ARGS__)
#define chimera_smb_abort(...) chimera_abort("smb", \
                                             __FILE__, \
                                             __LINE__, \
                                             __VA_ARGS__)

#define chimera_smb_fatal_if(cond, ...) \
        chimera_fatal_if(cond, "smb", \
                         __FILE__, \
                         __LINE__, \
                         __VA_ARGS__)

#define chimera_smb_abort_if(cond, ...) \
        chimera_abort_if(cond, "smb", \
                         __FILE__, \
                         __LINE__, \
                         __VA_ARGS__)


struct chimera_smb_config {
    char identity[80];
    int  port;
};

struct netbios_header {
    uint32_t word;
} __attribute__((packed));

struct chimera_smb_share {
    char                      name[81];
    char                      path[CHIMERA_VFS_PATH_MAX];
    struct chimera_smb_share *prev;
    struct chimera_smb_share *next;
};

struct chimera_smb_conn;

struct chimera_smb_request {
    uint32_t                     status;
    uint16_t                     request_struct_size;
    struct smb2_header           smb2_hdr;
    struct chimera_smb_session  *session;
    struct chimera_smb_tree     *tree;
    struct chimera_smb_compound *compound;
    struct chimera_smb_request  *next;

    union {

        struct {
            uint16_t dialect_count;
            uint8_t  security_mode;
            uint32_t capabilities;
            uint8_t  client_guid[16];
            uint32_t negotiate_context_offset;
            uint16_t negotiate_context_count;
            uint16_t r_dialect;
            uint16_t r_security_mode;
            uint8_t  r_server_guid[16];
            uint32_t r_capabilities;
            uint32_t r_max_transact_size;
            uint32_t r_max_read_size;
            uint32_t r_max_write_size;
            uint64_t r_system_time;
            uint64_t r_server_start_time;
            uint16_t dialects[SMB2_MAX_DIALECTS];
            struct {
                uint16_t type;
                uint16_t length;
            } negotiate_context[SMB2_MAX_NEGOTIATE_CONTEXTS];
        } negotiate;

        struct {
            uint8_t           flags;
            uint8_t           security_mode;
            uint16_t          input_niov;
            uint32_t          capabilities;
            uint32_t          channel;
            uint64_t          prev_session_id;
            uint16_t          blob_offset;
            uint16_t          blob_length;
            struct evpl_iovec input_iov[64];
        } session_setup;

        struct {
            uint16_t flags;
            uint16_t path_offset;
            uint16_t path_length;
            uint8_t  is_ipc;
            char     path[SMB_FILENAME_MAX];
        } tree_connect;

        struct {
            uint64_t                        flags;
            uint8_t                         requested_oplock_level;
            uint32_t                        impersonation_level;
            uint32_t                        desired_access;
            uint32_t                        file_attributes;
            uint32_t                        share_access;
            uint32_t                        create_disposition;
            uint32_t                        create_options;
            uint16_t                        parent_path_len;
            uint16_t                        name_len;
            struct chimera_vfs_open_handle *parent_handle;
            struct chimera_smb_open_file   *r_open_file;
            struct chimera_smb_attrs        r_attrs;
            struct chimera_vfs_attrs        set_attr;
            char                            parent_path[SMB_FILENAME_MAX];
            char                           *name;
        } create;

        struct  {
            uint16_t                        flags;
            struct chimera_smb_file_id      file_id;
            struct chimera_vfs_open_handle *handle;
            struct chimera_smb_attrs        r_attrs;
        } close;

        struct {
            uint64_t                   offset;
            uint32_t                   length;
            uint32_t                   channel;
            uint32_t                   remaining;
            uint32_t                   flags;
            uint32_t                   niov;
            struct chimera_smb_file_id file_id;
            struct evpl_iovec          iov[64];
        } write;

        struct {
            uint8_t                    flags;
            uint32_t                   length;
            uint32_t                   niov;
            uint64_t                   offset;
            uint32_t                   minimum;
            uint32_t                   channel;
            uint32_t                   remaining;
            uint32_t                   r_length;
            struct chimera_smb_file_id file_id;
            struct evpl_iovec          iov[64];
        } read;

        struct {
            struct chimera_smb_file_id file_id;
        } flush;

        struct {
            uint32_t                   ctl_code;
            struct chimera_smb_file_id file_id;
            uint32_t                   input_offset;
            uint32_t                   input_count;
            uint32_t                   max_input_response;
            uint32_t                   output_offset;
            uint32_t                   output_count;
            uint32_t                   max_output_response;
            uint32_t                   flags;
        } ioctl;
        struct {
            uint8_t                       info_type;
            uint8_t                       info_class;
            uint32_t                      addl_info;
            uint32_t                      flags;
            uint32_t                      output_length;
            struct chimera_smb_file_id    file_id;
            struct chimera_smb_attrs      r_attrs;
            struct chimera_smb_fs_attrs   r_fs_attrs;
            struct chimera_smb_open_file *open_file;
        } query_info;

        struct {
            uint8_t                         info_type;
            uint8_t                         info_class;
            uint32_t                        buffer_length;
            uint16_t                        buffer_offset;
            uint32_t                        addl_info;
            uint32_t                        flags;
            struct chimera_smb_open_file   *open_file;
            struct chimera_vfs_open_handle *parent_handle;
            struct chimera_smb_file_id      file_id;
            struct chimera_smb_attrs        attrs;
        } set_info;

        struct {
            uint8_t                       info_class;
            uint8_t                       flags;
            uint32_t                      file_index;
            uint8_t                       eof;
            uint16_t                      pattern_len;
            struct chimera_smb_file_id    file_id;
            uint16_t                      pattern_length;
            uint32_t                      output_length;
            uint32_t                      max_output_length;
            struct evpl_iovec             iov;
            struct chimera_smb_open_file *open_file;
            uint32_t                     *last_file_offset;
            char                          pattern[SMB_FILENAME_MAX];
        } query_directory;
    };
};

#define CHIMERA_SMB_COMPOUND_MAX_REQUESTS 64
struct chimera_smb_compound {
    int                               num_requests;
    int                               complete_requests;
    uint64_t                          saved_session_id;
    uint64_t                          saved_tree_id;
    struct chimera_smb_file_id        saved_file_id;
    struct chimera_server_smb_thread *thread;
    struct chimera_smb_conn          *conn;
    struct chimera_smb_compound      *next;
    struct chimera_smb_request       *requests[CHIMERA_SMB_COMPOUND_MAX_REQUESTS];
};

struct chimera_smb_session_handle {
    uint64_t                           session_id;
    struct chimera_smb_session        *session;
    struct UT_hash_handle              hh;
    struct chimera_smb_session_handle *next;
};

struct chimera_smb_conn {
    OM_uint32                          gss_major;
    OM_uint32                          gss_minor;
    gss_ctx_id_t                       ctx;
    gss_cred_id_t                      srv_cred;
    gss_buffer_desc                    gss_output;
    int                                established;
    struct chimera_smb_session        *last_session;
    struct chimera_smb_tree           *last_tree;
    struct chimera_smb_session_handle *session_handles;
    struct chimera_server_smb_thread  *thread;
    struct evpl_bind                  *bind;
    struct chimera_smb_conn           *prev;
    struct chimera_smb_conn           *next;
};

struct chimera_server_smb_shared {
    struct chimera_smb_config   config;
    uint8_t                     guid[SMB2_GUID_SIZE];
    struct chimera_vfs         *vfs;
    struct prometheus_metrics  *metrics;
    struct evpl_endpoint       *endpoint;
    struct evpl_listener       *listener;
    struct chimera_smb_session *sessions;
    struct chimera_smb_session *free_sessions;
    pthread_mutex_t             sessions_lock;
    struct chimera_smb_share   *shares;
    pthread_mutex_t             shares_lock;
    struct chimera_smb_tree    *free_trees;
    pthread_mutex_t             trees_lock;
};

struct chimera_server_smb_thread {
    struct evpl                       *evpl;
    struct chimera_vfs_thread         *vfs_thread;
    struct chimera_server_smb_shared  *shared;
    struct chimera_smb_request        *free_requests;
    struct chimera_smb_compound       *free_compounds;
    struct chimera_smb_conn           *free_conns;
    struct chimera_smb_session_handle *free_session_handles;
    struct chimera_smb_open_file      *free_open_files;
    struct chimera_smb_iconv_ctx       iconv_ctx;
};


static inline void
chimera_smb_tree_free(
    struct chimera_server_smb_shared *shared,
    struct chimera_smb_tree          *tree);

static inline struct chimera_smb_open_file *
chimera_smb_open_file_alloc(struct chimera_server_smb_thread *thread)
{
    struct chimera_smb_open_file *open_file = thread->free_open_files;

    if (open_file) {
        LL_DELETE(thread->free_open_files, open_file);
    } else {
        open_file = calloc(1, sizeof(*open_file));
    }

    return open_file;
} /* chimera_smb_open_file_alloc */

static inline void
chimera_smb_open_file_free(
    struct chimera_server_smb_thread *thread,
    struct chimera_smb_open_file     *open_file)
{
    LL_PREPEND(thread->free_open_files, open_file);
} /* chimera_smb_open_file_free */

static inline struct chimera_smb_request *
chimera_smb_request_alloc(struct chimera_server_smb_thread *thread)
{
    struct chimera_smb_request *request = thread->free_requests;

    if (request) {
        LL_DELETE(thread->free_requests, request);
    } else {
        request = calloc(1, sizeof(*request));
    }

    request->tree = NULL;

    return request;
} /* chimera_smb_request_alloc */

static inline void
chimera_smb_request_free(
    struct chimera_server_smb_thread *thread,
    struct chimera_smb_request       *request)
{
    LL_PREPEND(thread->free_requests, request);
} /* chimera_smb_request_free */

static inline struct chimera_smb_session *
chimera_smb_session_alloc(struct chimera_server_smb_shared *shared)
{
    struct chimera_smb_session *session;

    pthread_mutex_lock(&shared->sessions_lock);

    session = shared->free_sessions;

    if (session) {
        LL_DELETE(shared->free_sessions, session);
    } else {
        session = chimera_smb_session_create();
        pthread_mutex_init(&session->lock, NULL);
    }

    session->session_id = chimera_rand64();

    HASH_ADD(hh, shared->sessions, session_id, sizeof(uint64_t), session);

    pthread_mutex_unlock(&shared->sessions_lock);

    session->refcnt = 1;

    return session;
} /* chimera_smb_session_alloc */

static inline void
chimera_smb_session_release(
    struct chimera_server_smb_shared *shared,
    struct chimera_smb_session       *session)
{
    struct chimera_smb_tree *tree;
    int                      destroy;

    pthread_mutex_lock(&session->lock);
    session->refcnt--;
    destroy = session->refcnt == 0;
    pthread_mutex_unlock(&session->lock);

    if (destroy) {

        for (int i = 0; i < session->max_trees; i++) {
            tree = session->trees[i];

            if (tree) {
                chimera_smb_tree_free(shared, tree);
            }
        }

        pthread_mutex_lock(&shared->sessions_lock);

        HASH_DEL(shared->sessions, session);

        LL_PREPEND(shared->free_sessions, session);

        pthread_mutex_unlock(&shared->sessions_lock);
    }
} /* chimera_smb_session_free */


static inline struct chimera_smb_session_handle *
chimera_smb_session_handle_alloc(struct chimera_server_smb_thread *thread)
{
    struct chimera_smb_session_handle *session_handle = thread->free_session_handles;

    if (session_handle) {
        LL_DELETE(thread->free_session_handles, session_handle);
    } else {
        session_handle = calloc(1, sizeof(*session_handle));
    }

    return session_handle;
} /* chimera_smb_session_handle_alloc */

static inline void
chimera_smb_session_handle_free(
    struct chimera_server_smb_thread  *thread,
    struct chimera_smb_session_handle *session_handle)
{
    LL_PREPEND(thread->free_session_handles, session_handle);
} /* chimera_smb_session_handle_free */

static inline struct chimera_smb_conn *
chimera_smb_conn_alloc(struct chimera_server_smb_thread *thread)
{
    struct chimera_smb_conn *conn = thread->free_conns;

    if (conn) {
        LL_DELETE(thread->free_conns, conn);
    } else {
        conn = calloc(1, sizeof(*conn));
    }

    return conn;
} /* chimera_smb_conn_alloc */

static inline void
chimera_smb_conn_free(
    struct chimera_server_smb_thread *thread,
    struct chimera_smb_conn          *conn)
{
    struct chimera_smb_session_handle *session_handle, *tmp;

    HASH_ITER(hh, conn->session_handles, session_handle, tmp)
    {
        HASH_DELETE(hh, conn->session_handles, session_handle);

        chimera_smb_session_release(thread->shared, session_handle->session);

        chimera_smb_session_handle_free(thread, session_handle);
    }

    if (conn->ctx) {
        gss_delete_sec_context(&conn->gss_minor, &conn->ctx, NULL);
        conn->ctx = NULL;
    }

    if (conn->srv_cred) {
        gss_release_cred(&conn->gss_minor, &conn->srv_cred);
        conn->srv_cred = NULL;
    }

    if (conn->gss_output.value) {
        gss_release_buffer(&conn->gss_minor, &conn->gss_output);
        conn->gss_output.value  = NULL;
        conn->gss_output.length = 0;
    }

    LL_PREPEND(thread->free_conns, conn);
} /* chimera_smb_conn_free */


static inline struct chimera_smb_tree *
chimera_smb_tree_alloc(struct chimera_server_smb_shared *shared)
{
    struct chimera_smb_tree *tree;

    pthread_mutex_lock(&shared->trees_lock);

    tree = shared->free_trees;

    if (tree) {
        LL_DELETE(shared->free_trees, tree);
    } else {
        tree = calloc(1, sizeof(*tree));

        for (int i = 0; i < CHIMERA_SMB_OPEN_FILE_BUCKETS; i++) {
            pthread_mutex_init(&tree->open_files_lock[i], NULL);
        }
    }

    tree->fh_expiration.tv_sec  = 0;
    tree->fh_expiration.tv_nsec = 0;
    tree->refcnt                = 1;

    pthread_mutex_unlock(&shared->trees_lock);
    return tree;
} /* chimera_smb_tree_alloc */

static inline void
chimera_smb_tree_free(
    struct chimera_server_smb_shared *shared,
    struct chimera_smb_tree          *tree)
{
    pthread_mutex_lock(&shared->trees_lock);

    LL_PREPEND(shared->free_trees, tree);

    pthread_mutex_unlock(&shared->trees_lock);
} /* chimera_smb_tree_free */

static inline struct chimera_smb_compound *
chimera_smb_compound_alloc(struct chimera_server_smb_thread *thread)
{
    struct chimera_smb_compound *compound = thread->free_compounds;

    if (compound) {
        LL_DELETE(thread->free_compounds, compound);
    } else {
        compound = calloc(1, sizeof(*compound));
    }

    return compound;
} /* chimera_smb_compound_alloc */

static inline void
chimera_smb_compound_free(
    struct chimera_server_smb_thread *thread,
    struct chimera_smb_compound      *compound)
{
    LL_PREPEND(thread->free_compounds, compound);
} /* chimera_smb_compound_free */

static inline struct chimera_smb_open_file *
chimera_smb_open_file_lookup(
    struct chimera_smb_request *request,
    struct chimera_smb_file_id *file_id)
{
    struct chimera_smb_open_file *open_file;
    struct chimera_smb_tree      *tree = request->tree;
    int                           open_file_bucket;

    chimera_smb_abort_if(!tree, "tree is NULL");

    if (file_id->pid == UINT64_MAX) {
        if (request->compound->saved_file_id.pid == UINT64_MAX) {
            chimera_smb_error("Attempted to lookup invalid file id");
            return NULL;
        }
        file_id->pid = request->compound->saved_file_id.pid;
    }

    if (file_id->vid == UINT64_MAX) {
        if (request->compound->saved_file_id.vid == UINT64_MAX) {
            chimera_smb_error("Attempted to lookup invalid file id");
            return NULL;
        }
        file_id->vid = request->compound->saved_file_id.vid;
    }

    open_file_bucket = file_id->vid & CHIMERA_SMB_OPEN_FILE_BUCKET_MASK;

    pthread_mutex_lock(&tree->open_files_lock[open_file_bucket]);

    HASH_FIND(hh, tree->open_files[open_file_bucket], file_id, sizeof(*file_id), open_file);

    pthread_mutex_unlock(&tree->open_files_lock[open_file_bucket]);

    chimera_smb_abort_if(!open_file, "open request for file id %lx.%lx did not match an open file",
                         file_id->pid, file_id->vid);

    return open_file;
} /* chimera_smb_open_file_find */


static inline struct chimera_smb_open_file *
chimera_smb_open_file_remove(
    struct chimera_smb_request *request,
    struct chimera_smb_file_id *file_id)
{
    struct chimera_smb_open_file *open_file;
    struct chimera_smb_tree      *tree = request->tree;
    int                           open_file_bucket;

    chimera_smb_abort_if(!tree, "tree is NULL");

    if (file_id->pid == UINT64_MAX) {
        if (request->compound->saved_file_id.pid == UINT64_MAX) {
            chimera_smb_error("Attempted to close invalid file id");
            return NULL;
        }
        file_id->pid = request->compound->saved_file_id.pid;
    }

    if (file_id->vid == UINT64_MAX) {
        if (request->compound->saved_file_id.vid == UINT64_MAX) {
            chimera_smb_error("Attempted to close invalid file id");
            return NULL;
        }
        file_id->vid = request->compound->saved_file_id.vid;
    }

    open_file_bucket = file_id->vid & CHIMERA_SMB_OPEN_FILE_BUCKET_MASK;

    pthread_mutex_lock(&tree->open_files_lock[open_file_bucket]);

    HASH_FIND(hh, tree->open_files[open_file_bucket], file_id, sizeof(*file_id), open_file);

    if (open_file) {
        HASH_DELETE(hh, tree->open_files[open_file_bucket], open_file);
    }

    pthread_mutex_unlock(&tree->open_files_lock[open_file_bucket]);

    return open_file;
} /* chimera_smb_open_file_find */