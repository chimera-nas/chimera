// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <stdint.h>
#include <pthread.h>
#include <uthash.h>

#include "vfs/vfs.h"
#include "vfs/vfs_cred.h"
#include "smb2.h"

struct chimera_smb_share;

struct chimera_smb_file_id {
    uint64_t pid;
    uint64_t vid;
};

#define CHIMERA_SMB_OPEN_FILE_FLAG_DIRECTORY       0x00000001
#define CHIMERA_SMB_OPEN_FILE_FLAG_DELETE_ON_CLOSE 0x00000002
#define CHIMERA_SMB_OPEN_FILE_CLOSED               0x00000004

/* Bits identifying which CREATE contexts a client supplied on the open. Mirrored
 * from request->create.ctx_present_mask into the open file so later phases
 * (lease break, durable reconnect) can tell which contracts the client expects. */
#define CHIMERA_SMB_CREATE_CTX_SECD                (1u << 0)
#define CHIMERA_SMB_CREATE_CTX_EXTA                (1u << 1)
#define CHIMERA_SMB_CREATE_CTX_DHNQ                (1u << 2)
#define CHIMERA_SMB_CREATE_CTX_DHNC                (1u << 3)
#define CHIMERA_SMB_CREATE_CTX_DH2Q                (1u << 4)
#define CHIMERA_SMB_CREATE_CTX_DH2C                (1u << 5)
#define CHIMERA_SMB_CREATE_CTX_RQLS                (1u << 6)
#define CHIMERA_SMB_CREATE_CTX_RQLS_V2             (1u << 7)
#define CHIMERA_SMB_CREATE_CTX_MXAC                (1u << 8)
#define CHIMERA_SMB_CREATE_CTX_TWRP                (1u << 9)
#define CHIMERA_SMB_CREATE_CTX_QFID                (1u << 10)
#define CHIMERA_SMB_CREATE_CTX_ALSI                (1u << 11)
/* App-instance contexts (SMB2_CREATE_APP_INSTANCE_ID / *_VERSION) use 16-byte
 * GUID names and so are not reachable from the 4-byte tag dispatch. Phase 3
 * (durable / app-instance handle replacement) will add a GUID-name match path
 * and re-introduce a CHIMERA_SMB_CREATE_CTX_APP bit at that point. */

/* Bits for chimera_smb_open_file.durable_flags */
#define CHIMERA_SMB_DURABLE_V1                     (1u << 0)
#define CHIMERA_SMB_DURABLE_V2                     (1u << 1)
#define CHIMERA_SMB_DURABLE_PERSISTENT             (1u << 2)

enum chimera_smb_open_file_type {
    CHIMERA_SMB_OPEN_FILE_TYPE_FILE,
    CHIMERA_SMB_OPEN_FILE_TYPE_PIPE,
};

enum chimera_smb_pipe_magic {
    CHIMERA_SMB_OPEN_FILE_LSA_RPC,
};

struct chimera_smb_request;

typedef int (*chimera_smb_pipe_transceive_t)(
    struct chimera_smb_request *request,
    struct evpl_iovec          *input_iov,
    int                         input_niov,
    struct evpl_iovec          *output_iov);

struct chimera_smb_notify_state;

struct chimera_smb_open_file {
    enum chimera_smb_open_file_type  type;
    enum chimera_smb_pipe_magic      pipe_magic;
    chimera_smb_pipe_transceive_t    pipe_transceive;
    struct UT_hash_handle            hh;
    struct chimera_smb_file_id       file_id;
    struct chimera_vfs_open_handle  *handle;
    uint32_t                         desired_access;
    uint32_t                         share_access;
    uint32_t                         name_len;
    uint32_t                         flags;
    uint64_t                         position;
    uint32_t                         parent_fh_len;
    uint32_t                         refcnt;
    /* Phase-0 plumbing: fields populated by later phases. Zeroed on alloc. */
    uint32_t                         ctx_present_mask;
    uint8_t                          oplock_level;
    uint8_t                          lease_state;
    uint16_t                         lease_epoch;
    uint32_t                         lease_flags;
    uint32_t                         durable_flags;
    uint64_t                         durable_timeout_ms;
    uint8_t                          lease_key[16];
    uint8_t                          parent_lease_key[16];
    uint8_t                          create_guid[16];
    struct chimera_smb_open_file    *next;
    struct chimera_smb_notify_state *notify_state;
    uint8_t                          parent_fh[CHIMERA_VFS_FH_SIZE];
    char                             name[SMB_FILENAME_MAX];
    uint16_t                         pattern[SMB_FILENAME_MAX];
};

#define CHIMERA_SMB_OPEN_FILE_BUCKETS     256
#define CHIMERA_SMB_OPEN_FILE_BUCKET_MASK (CHIMERA_SMB_OPEN_FILE_BUCKETS - 1)

enum chimera_smb_tree_type {
    CHIMERA_SMB_TREE_TYPE_PIPE,
    CHIMERA_SMB_TREE_TYPE_SHARE,
};

struct chimera_smb_tree {
    enum chimera_smb_tree_type    type;
    uint32_t                      tree_id;
    uint32_t                      refcnt;
    uint64_t                      next_file_id;
    struct chimera_smb_share     *share;

    struct chimera_smb_open_file *open_files[CHIMERA_SMB_OPEN_FILE_BUCKETS];
    pthread_mutex_t               open_files_lock[CHIMERA_SMB_OPEN_FILE_BUCKETS];

    struct chimera_smb_tree      *prev;
    struct chimera_smb_tree      *next;

    unsigned int                  fh_len;
    struct timespec               fh_expiration;
    uint8_t                       fh[CHIMERA_VFS_FH_SIZE];
};

#define CHIMERA_SMB_SESSION_AUTHORIZED 0x1

struct chimera_smb_session {
    uint64_t                    session_id;
    uint32_t                    refcnt;
    uint32_t                    flags;
    struct UT_hash_handle       hh;
    struct chimera_smb_session *prev;
    struct chimera_smb_session *next;

    pthread_mutex_t             lock;
    struct chimera_smb_tree   **trees;

    int                         max_trees;
    uint8_t                     signing_key[16];

    struct chimera_vfs_cred     cred;
};

static struct chimera_smb_session *
chimera_smb_session_create()
{
    struct chimera_smb_session *session = calloc(1, sizeof(struct chimera_smb_session));

    pthread_mutex_init(&session->lock, NULL);

    session->max_trees = 32;
    session->flags     = 0;

    session->trees = calloc(session->max_trees, sizeof(struct chimera_smb_tree *));

    /* Initialize credentials to root for now.
     * TODO: Map authenticated SMB user to appropriate UID/GID
     */
    chimera_vfs_cred_init_attr(&session->cred, 0, 0, 0, NULL);

    return session;
} /* chimera_smb_session_create */

static void
chimera_smb_session_destroy(struct chimera_smb_session *session)
{
    pthread_mutex_destroy(&session->lock);
    free(session->trees);
    free(session);
} /* chimera_smb_session_release */
