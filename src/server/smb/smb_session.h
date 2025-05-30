#pragma once

#include <stdint.h>
#include <pthread.h>

#include "uthash/uthash.h"
#include "vfs/vfs.h"
#include "smb2.h"

struct chimera_smb_share;

struct chimera_smb_file_id {
    uint64_t pid;
    uint64_t vid;
};

#define CHIMERA_SMB_OPEN_FILE_FLAG_DIRECTORY       0x00000001
#define CHIMERA_SMB_OPEN_FILE_FLAG_DELETE_ON_CLOSE 0x00000002

struct chimera_smb_open_file {
    struct UT_hash_handle           hh;
    struct chimera_smb_file_id      file_id;
    struct chimera_vfs_open_handle *handle;
    uint32_t                        name_len;
    uint32_t                        flags;
    uint64_t                        position;
    struct chimera_smb_open_file   *next;
    uint16_t                        name[SMB_FILENAME_MAX];
    uint16_t                        pattern[SMB_FILENAME_MAX];
};

#define CHIMERA_SMB_OPEN_FILE_BUCKETS     256
#define CHIMERA_SMB_OPEN_FILE_BUCKET_MASK (CHIMERA_SMB_OPEN_FILE_BUCKETS - 1)

struct chimera_smb_tree {
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

struct chimera_smb_session {
    uint64_t                    session_id;
    uint64_t                    refcnt;
    struct UT_hash_handle       hh;
    struct chimera_smb_session *prev;
    struct chimera_smb_session *next;

    pthread_mutex_t             lock;
    struct chimera_smb_tree   **trees;

    int                         max_trees;
};

static struct chimera_smb_session *
chimera_smb_session_create()
{
    struct chimera_smb_session *session = calloc(1, sizeof(struct chimera_smb_session));

    pthread_mutex_init(&session->lock, NULL);

    session->max_trees = 32;

    session->trees = calloc(session->max_trees, sizeof(struct chimera_smb_tree *));

    return session;
} /* chimera_smb_session_create */

static void
chimera_smb_session_destroy(struct chimera_smb_session *session)
{
    pthread_mutex_destroy(&session->lock);
    free(session->trees);
    free(session);
} /* chimera_smb_session_release */