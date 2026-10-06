// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include "smb_internal.h"
#include "common/compound_retry.h"
#include "smb_procs.h"
#include "smb_common/smb2.h"
#include "vfs/vfs.h"
#include "vfs/vfs_compound.h"
#include "vfs/vfs_release.h"

/*
 * Object IDs (MS-FSA 2.1.5.10.1/.2/.13/.35/.36; MS-FSCC 2.1.3).
 *
 * A file's FILE_OBJECTID_BUFFER -- ObjectId, BirthVolumeId, BirthObjectId and
 * DomainId, 16 bytes each -- is its chimera.objectid xattr, outside the user.
 * namespace so it is neither an EA nor an NFS user xattr.  An ObjectId must be
 * unique on the volume, so the share root keeps an index: an xattr named
 * chimera.objid.<hex ObjectId> holding the file handle of the file that has
 * it.  A file removed without deleting its ID leaves its index entry behind;
 * an entry whose file is gone, or no longer has that ID, counts as free.
 *
 * On a backend without xattrs only CREATE_OR_GET_OBJECT_ID answers, with an ID
 * derived from the file handle that is stable but not stored.
 */

#define CHIMERA_SMB_OID_XATTR        "chimera.objectid"
#define CHIMERA_SMB_OID_XATTR_LEN    (sizeof(CHIMERA_SMB_OID_XATTR) - 1)
#define CHIMERA_SMB_OID_INDEX_PREFIX "chimera.objid."
#define CHIMERA_SMB_OID_SIZE         64

static const uint8_t chimera_smb_oid_zero[16];

static void chimera_smb_oid_store(
    struct chimera_smb_request *request);

static void
chimera_smb_oid_finish(
    struct chimera_smb_request *request,
    uint32_t                    status)
{
    struct chimera_vfs_thread *vfs_thread = request->compound->thread->vfs_thread;

    if (request->ioctl.oid_root) {
        chimera_vfs_release(vfs_thread, request->ioctl.oid_root);
        request->ioctl.oid_root = NULL;
    }
    if (request->ioctl.oid_other_handle) {
        chimera_vfs_release(vfs_thread, request->ioctl.oid_other_handle);
        request->ioctl.oid_other_handle = NULL;
    }
    chimera_smb_open_file_release(request, request->ioctl.oid_open_file);
    chimera_smb_complete_request(request, status);
} /* chimera_smb_oid_finish */

static uint32_t
chimera_smb_oid_vfs_status(enum chimera_vfs_error error_code)
{
    switch (error_code) {
        case CHIMERA_VFS_OK:
            return SMB2_STATUS_SUCCESS;
        case CHIMERA_VFS_ENOTSUP:
            return SMB2_STATUS_INVALID_DEVICE_REQUEST;
        case CHIMERA_VFS_EACCES:
        case CHIMERA_VFS_EPERM:
            return SMB2_STATUS_ACCESS_DENIED;
        default:
            return SMB2_STATUS_INTERNAL_ERROR;
    } /* switch */
} /* chimera_smb_oid_vfs_status */

/* The index entry naming the ObjectId in oid_buffer. */
static void
chimera_smb_oid_index_name(struct chimera_smb_request *request)
{
    char *p = request->ioctl.oid_index_name;
    int   i;

    p += sprintf(p, "%s", CHIMERA_SMB_OID_INDEX_PREFIX);
    for (i = 0; i < 16; i++) {
        p += sprintf(p, "%02x", request->ioctl.oid_buffer[i]);
    }
} /* chimera_smb_oid_index_name */

/* The ID CREATE_OR_GET hands out for a file with none: stable for the file,
 * derived from its handle and the server GUID (the BirthVolumeId). */
static void
chimera_smb_oid_generate(
    struct chimera_smb_request   *request,
    struct chimera_smb_open_file *open_file)
{
    struct chimera_server_smb_shared *shared = request->compound->thread->shared;
    uint8_t                          *b      = request->ioctl.oid_buffer;

    memset(b, 0, CHIMERA_SMB_OID_SIZE);
    if (open_file->handle) {
        memcpy(b, &open_file->handle->fh_hash, 8);
        memcpy(b + 8, &open_file->handle->fh_hash, 8);
        b[8] ^= 0xa5;
        memcpy(b + 16, shared->guid, 16);  /* BirthVolumeId */
        memcpy(b + 32, b, 16);             /* BirthObjectId */
    }
} /* chimera_smb_oid_generate */

/* ---- the index on the share root ---- */

static void
chimera_smb_oid_index_set_cb(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct chimera_smb_request *request = private_data;

    enum chimera_vfs_error      error_code = chimera_vfs_compound_status(compound);

    chimera_vfs_compound_free(compound);
    request->vfs_compound = NULL;

    (void) error_code;
    /* The ID is stored on the file; a failed index update only loses the
     * duplicate check for it. */
    chimera_smb_oid_finish(private_data, SMB2_STATUS_SUCCESS);
} /* chimera_smb_oid_index_set_cb */

static void
chimera_smb_oid_index_removed_cb(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct chimera_smb_request *request = private_data;

    enum chimera_vfs_error      error_code = chimera_vfs_compound_status(compound);

    chimera_vfs_compound_free(compound);
    request->vfs_compound = NULL;

    (void) error_code;
    chimera_smb_oid_finish(private_data, SMB2_STATUS_SUCCESS);
} /* chimera_smb_oid_index_removed_cb */

/* The share root is open: add or drop this file's index entry. */
static void
chimera_smb_oid_root_open_cb(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct chimera_smb_request     *request = private_data;

    enum chimera_vfs_error          error_code = chimera_vfs_compound_status(compound);
    struct chimera_vfs_open_handle *oh         = error_code == CHIMERA_VFS_OK ? chimera_vfs_compound_take_handle(
        compound, chimera_vfs_compound_num_ops(compound) - 1) : NULL;

    chimera_vfs_compound_free(compound);
    request->vfs_compound = NULL;
    struct chimera_vfs_thread      *vfs_thread = request->compound->thread->vfs_thread;
    struct chimera_smb_open_file   *open_file  = request->ioctl.oid_open_file;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_smb_oid_finish(request, SMB2_STATUS_SUCCESS);
        return;
    }
    request->ioctl.oid_root = oh;

    if (request->ioctl.ctl_code == SMB2_FSCTL_DELETE_OBJECT_ID) {
        {
            struct chimera_vfs_compound *sequence = chimera_vfs_compound_alloc(vfs_thread, &request->session_handle->
                                                                               session->cred);
            request->vfs_compound = sequence;
            chimera_vfs_compound_add_puthandle(sequence, oh, CHIMERA_VFS_OPEN_PATH);
            chimera_vfs_compound_add_removexattr(sequence, request->ioctl.oid_index_name, strlen(request->ioctl.
                                                                                                 oid_index_name));
            chimera_frontend_compound_submit(sequence, chimera_smb_oid_index_removed_cb, request);
        }
        return;
    }

    {
        struct chimera_vfs_compound *sequence = chimera_vfs_compound_alloc(vfs_thread, &request->session_handle->session
                                                                           ->cred);
        request->vfs_compound = sequence;
        chimera_vfs_compound_add_puthandle(sequence, oh, CHIMERA_VFS_OPEN_PATH);
        chimera_vfs_compound_add_setxattr(sequence, 0, request->ioctl.oid_index_name, strlen(request->ioctl.
                                                                                             oid_index_name), open_file
                                          ->handle->fh, open_file->handle->fh_len);
        chimera_frontend_compound_submit(sequence, chimera_smb_oid_index_set_cb, request);
    }
} /* chimera_smb_oid_root_open_cb */

static void
chimera_smb_oid_index_update(struct chimera_smb_request *request)
{
    chimera_smb_oid_index_name(request);
    {
        struct chimera_vfs_compound *sequence = chimera_vfs_compound_alloc(request->compound->thread->vfs_thread, &
                                                                           request->session_handle->session->cred);
        request->vfs_compound = sequence;
        chimera_vfs_compound_add_putfh(sequence, request->tree->fh, request->tree->fh_len);
        chimera_vfs_compound_add_open_current(sequence, CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_INFERRED |
                                              CHIMERA_VFS_OPEN_DIRECTORY, 0);
        chimera_vfs_compound_add_gethandle(sequence);
        chimera_frontend_compound_submit(sequence, chimera_smb_oid_root_open_cb, request);
    }
} /* chimera_smb_oid_index_update */

/* ---- storing the file's ID ---- */

static void
chimera_smb_oid_stored_cb(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct chimera_smb_request *request = private_data;

    enum chimera_vfs_error      error_code = chimera_vfs_compound_status(compound);

    chimera_vfs_compound_free(compound);
    request->vfs_compound = NULL;


    if (error_code != CHIMERA_VFS_OK) {
        chimera_smb_oid_finish(request, chimera_smb_oid_vfs_status(error_code));
        return;
    }
    /* SET_EXTENDED changes only the birth/domain IDs, not the indexed one. */
    if (request->ioctl.ctl_code == SMB2_FSCTL_SET_OBJECT_ID_EXTENDED) {
        chimera_smb_oid_finish(request, SMB2_STATUS_SUCCESS);
        return;
    }
    chimera_smb_oid_index_update(request);
} /* chimera_smb_oid_stored_cb */

static void
chimera_smb_oid_store(struct chimera_smb_request *request)
{
    {
        struct chimera_vfs_compound *sequence = chimera_vfs_compound_alloc(request->compound->thread->vfs_thread, &
                                                                           request->session_handle->session->cred);
        request->vfs_compound = sequence;
        chimera_vfs_compound_add_puthandle(sequence, request->ioctl.oid_open_file->handle, CHIMERA_VFS_OPEN_PATH);
        chimera_vfs_compound_add_setxattr(sequence, 0, CHIMERA_SMB_OID_XATTR, CHIMERA_SMB_OID_XATTR_LEN, request->ioctl.
                                          oid_buffer, CHIMERA_SMB_OID_SIZE);
        chimera_frontend_compound_submit(sequence, chimera_smb_oid_stored_cb, request);
    }
} /* chimera_smb_oid_store */

/* ---- SET: the ObjectId must not be another live file's ---- */

static void
chimera_smb_oid_other_id_cb(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct chimera_smb_request           *request = private_data;

    enum chimera_vfs_error                error_code = chimera_vfs_compound_status(compound);
    const struct chimera_vfs_compound_op *result     = chimera_vfs_compound_op(compound, chimera_vfs_compound_num_ops(
                                                                                   compound) - 1);
    uint32_t                              value_len = error_code == CHIMERA_VFS_OK ? result->buffer_len : 0;

    if (value_len) {
        memcpy(request->ioctl.rp_response, result->buffer, value_len);
    }
    chimera_vfs_compound_free(compound);
    request->vfs_compound = NULL;

    /* rp_response holds the other file's ID; the index entry is live only if
     * that file still has this one. */
    if (error_code == CHIMERA_VFS_OK && value_len >= 16 &&
        memcmp(request->ioctl.rp_response, request->ioctl.oid_buffer, 16) == 0) {
        chimera_smb_oid_finish(request, SMB2_STATUS_DUPLICATE_NAME);
        return;
    }
    chimera_vfs_release(request->compound->thread->vfs_thread, request->ioctl.oid_other_handle);
    request->ioctl.oid_other_handle = NULL;
    chimera_smb_oid_store(request);
} /* chimera_smb_oid_other_id_cb */

static void
chimera_smb_oid_other_open_cb(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct chimera_smb_request     *request = private_data;

    enum chimera_vfs_error          error_code = chimera_vfs_compound_status(compound);
    struct chimera_vfs_open_handle *oh         = error_code == CHIMERA_VFS_OK ? chimera_vfs_compound_take_handle(
        compound, chimera_vfs_compound_num_ops(compound) - 1) : NULL;

    chimera_vfs_compound_free(compound);
    request->vfs_compound = NULL;

    if (error_code != CHIMERA_VFS_OK) {
        /* The file the index names is gone: the entry is stale. */
        chimera_smb_oid_store(request);
        return;
    }
    request->ioctl.oid_other_handle = oh;
    {
        struct chimera_vfs_compound *sequence = chimera_vfs_compound_alloc(request->compound->thread->vfs_thread, &
                                                                           request->session_handle->session->cred);
        request->vfs_compound = sequence;
        chimera_vfs_compound_add_puthandle(sequence, oh, CHIMERA_VFS_OPEN_PATH);
        chimera_vfs_compound_add_getxattr(sequence, CHIMERA_SMB_OID_XATTR, CHIMERA_SMB_OID_XATTR_LEN,
                                          CHIMERA_SMB_OID_SIZE);
        chimera_frontend_compound_submit(sequence, chimera_smb_oid_other_id_cb, request);
    }
} /* chimera_smb_oid_other_open_cb */

static void
chimera_smb_oid_index_get_cb(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct chimera_smb_request           *request = private_data;

    enum chimera_vfs_error                error_code = chimera_vfs_compound_status(compound);
    const struct chimera_vfs_compound_op *result     = chimera_vfs_compound_op(compound, chimera_vfs_compound_num_ops(
                                                                                   compound) - 1);
    uint32_t                              value_len = error_code == CHIMERA_VFS_OK ? result->buffer_len : 0;

    if (value_len) {
        memcpy(request->ioctl.oid_fh, result->buffer, value_len);
    }
    chimera_vfs_compound_free(compound);
    request->vfs_compound = NULL;
    struct chimera_smb_open_file         *open_file = request->ioctl.oid_open_file;

    chimera_vfs_release(request->compound->thread->vfs_thread, request->ioctl.oid_root);
    request->ioctl.oid_root = NULL;

    if (error_code != CHIMERA_VFS_OK || value_len == 0 ||
        value_len > sizeof(request->ioctl.oid_fh) ||
        (value_len == open_file->handle->fh_len &&
         memcmp(request->ioctl.oid_fh, open_file->handle->fh, value_len) == 0)) {
        chimera_smb_oid_store(request);
        return;
    }

    request->ioctl.oid_fh_len = value_len;
    {
        struct chimera_vfs_compound *sequence = chimera_vfs_compound_alloc(request->compound->thread->vfs_thread, &
                                                                           request->session_handle->session->cred);
        request->vfs_compound = sequence;
        chimera_vfs_compound_add_putfh(sequence, request->ioctl.oid_fh, value_len);
        chimera_vfs_compound_add_open_current(sequence, CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_INFERRED, 0);
        chimera_vfs_compound_add_gethandle(sequence);
        chimera_frontend_compound_submit(sequence, chimera_smb_oid_other_open_cb, request);
    }
} /* chimera_smb_oid_index_get_cb */

static void
chimera_smb_oid_unique_root_cb(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct chimera_smb_request     *request = private_data;

    enum chimera_vfs_error          error_code = chimera_vfs_compound_status(compound);
    struct chimera_vfs_open_handle *oh         = error_code == CHIMERA_VFS_OK ? chimera_vfs_compound_take_handle(
        compound, chimera_vfs_compound_num_ops(compound) - 1) : NULL;

    chimera_vfs_compound_free(compound);
    request->vfs_compound = NULL;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_smb_oid_store(request);
        return;
    }
    request->ioctl.oid_root = oh;
    {
        struct chimera_vfs_compound *sequence = chimera_vfs_compound_alloc(request->compound->thread->vfs_thread, &
                                                                           request->session_handle->session->cred);
        request->vfs_compound = sequence;
        chimera_vfs_compound_add_puthandle(sequence, oh, CHIMERA_VFS_OPEN_PATH);
        chimera_vfs_compound_add_getxattr(sequence, request->ioctl.oid_index_name, strlen(request->ioctl.oid_index_name)
                                          , sizeof(request->ioctl.oid_fh));
        chimera_frontend_compound_submit(sequence, chimera_smb_oid_index_get_cb, request);
    }
} /* chimera_smb_oid_unique_root_cb */

static void
chimera_smb_oid_check_unique(struct chimera_smb_request *request)
{
    chimera_smb_oid_index_name(request);
    {
        struct chimera_vfs_compound *sequence = chimera_vfs_compound_alloc(request->compound->thread->vfs_thread, &
                                                                           request->session_handle->session->cred);
        request->vfs_compound = sequence;
        chimera_vfs_compound_add_putfh(sequence, request->tree->fh, request->tree->fh_len);
        chimera_vfs_compound_add_open_current(sequence, CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_INFERRED |
                                              CHIMERA_VFS_OPEN_DIRECTORY, 0);
        chimera_vfs_compound_add_gethandle(sequence);
        chimera_frontend_compound_submit(sequence, chimera_smb_oid_unique_root_cb, request);
    }
} /* chimera_smb_oid_check_unique */

/* ---- the file's current ID decides each operation ---- */

static void
chimera_smb_oid_current_cb(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct chimera_smb_request           *request = private_data;

    enum chimera_vfs_error                error_code = chimera_vfs_compound_status(compound);
    const struct chimera_vfs_compound_op *result     = chimera_vfs_compound_op(compound, chimera_vfs_compound_num_ops(
                                                                                   compound) - 1);
    uint32_t                              value_len = error_code == CHIMERA_VFS_OK ? result->buffer_len : 0;

    if (value_len) {
        memcpy(request->ioctl.oid_buffer, result->buffer, value_len);
    }
    chimera_vfs_compound_free(compound);
    request->vfs_compound = NULL;
    int                                   have = error_code == CHIMERA_VFS_OK &&
        value_len == CHIMERA_SMB_OID_SIZE;

    if (error_code != CHIMERA_VFS_OK && error_code != CHIMERA_VFS_ENODATA) {
        chimera_smb_oid_finish(request, chimera_smb_oid_vfs_status(error_code));
        return;
    }

    switch (request->ioctl.ctl_code) {
        case SMB2_FSCTL_GET_OBJECT_ID:
            chimera_smb_oid_finish(request, have ? SMB2_STATUS_SUCCESS :
                                   SMB2_STATUS_OBJECTID_NOT_FOUND);
            return;

        case SMB2_FSCTL_SET_OBJECT_ID:
            if (have) {
                chimera_smb_oid_finish(request, SMB2_STATUS_OBJECT_NAME_COLLISION);
                return;
            }
            memcpy(request->ioctl.oid_buffer, request->ioctl.oid_in, CHIMERA_SMB_OID_SIZE);
            chimera_smb_oid_check_unique(request);
            return;

        case SMB2_FSCTL_SET_OBJECT_ID_EXTENDED:
            if (!have) {
                chimera_smb_oid_finish(request, SMB2_STATUS_OBJECTID_NOT_FOUND);
                return;
            }
            memcpy(request->ioctl.oid_buffer + 16, request->ioctl.oid_in, 48);
            chimera_smb_oid_store(request);
            return;

        case SMB2_FSCTL_DELETE_OBJECT_ID:
            if (!have) {
                chimera_smb_oid_finish(request, SMB2_STATUS_SUCCESS);
                return;
            }
            /* oid_buffer still holds the ID being removed, which names the
             * index entry to drop once the file's copy is gone. */
            {
                struct chimera_vfs_compound *sequence = chimera_vfs_compound_alloc(request->compound->thread->vfs_thread
                                                                                   , &request->session_handle->session->
                                                                                   cred);
                request->vfs_compound = sequence;
                chimera_vfs_compound_add_puthandle(sequence, request->ioctl.oid_open_file->handle, CHIMERA_VFS_OPEN_PATH
                                                   );
                chimera_vfs_compound_add_removexattr(sequence, CHIMERA_SMB_OID_XATTR, CHIMERA_SMB_OID_XATTR_LEN);
                chimera_frontend_compound_submit(sequence, chimera_smb_oid_stored_cb, request);
            }
            return;

        case SMB2_FSCTL_CREATE_OR_GET_OBJECT_ID:
        default:
            if (have) {
                /* Fill birth IDs a bare SET left empty (MS-FSA 2.1.5.10.1). */
                if (memcmp(request->ioctl.oid_buffer + 16, chimera_smb_oid_zero, 16) == 0 &&
                    memcmp(request->ioctl.oid_buffer + 32, chimera_smb_oid_zero, 16) == 0) {
                    memcpy(request->ioctl.oid_buffer + 16,
                           request->compound->thread->shared->guid, 16);
                    memcpy(request->ioctl.oid_buffer + 32, request->ioctl.oid_buffer, 16);
                    memset(request->ioctl.oid_buffer + 48, 0, 16);
                    {
                        struct chimera_vfs_compound *sequence = chimera_vfs_compound_alloc(request->compound->thread->
                                                                                           vfs_thread, &request->
                                                                                           session_handle->session->cred
                                                                                           );
                        request->vfs_compound = sequence;
                        chimera_vfs_compound_add_puthandle(sequence, request->ioctl.oid_open_file->handle,
                                                           CHIMERA_VFS_OPEN_PATH);
                        chimera_vfs_compound_add_setxattr(sequence, 0, CHIMERA_SMB_OID_XATTR, CHIMERA_SMB_OID_XATTR_LEN,
                                                          request->ioctl.oid_buffer, CHIMERA_SMB_OID_SIZE);
                        chimera_frontend_compound_submit(sequence, chimera_smb_oid_index_set_cb, request);
                    }
                    return;
                }
                chimera_smb_oid_finish(request, SMB2_STATUS_SUCCESS);
                return;
            }
            chimera_smb_oid_generate(request, request->ioctl.oid_open_file);
            chimera_smb_oid_store(request);
            return;
    } /* switch */
} /* chimera_smb_oid_current_cb */

void
chimera_smb_ioctl_object_id(struct chimera_smb_request *request)
{
    struct chimera_smb_open_file *open_file;
    uint32_t                      ctl = request->ioctl.ctl_code;

    open_file = chimera_smb_open_file_resolve(request, &request->ioctl.file_id);
    if (!open_file) {
        chimera_smb_complete_request(request, SMB2_STATUS_FILE_CLOSED);
        return;
    }
    request->ioctl.oid_open_file    = open_file;
    request->ioctl.oid_root         = NULL;
    request->ioctl.oid_other_handle = NULL;

    if ((ctl == SMB2_FSCTL_GET_OBJECT_ID || ctl == SMB2_FSCTL_CREATE_OR_GET_OBJECT_ID) &&
        request->ioctl.max_output_response < CHIMERA_SMB_OID_SIZE) {
        chimera_smb_oid_finish(request, SMB2_STATUS_INVALID_PARAMETER);
        return;
    }

    if (ctl == SMB2_FSCTL_SET_OBJECT_ID_EXTENDED &&
        !(open_file->granted_access & (SMB2_FILE_WRITE_DATA | SMB2_FILE_WRITE_ATTRIBUTES))) {
        chimera_smb_oid_finish(request, SMB2_STATUS_ACCESS_DENIED);
        return;
    }

    /* Setting or deleting an object ID takes restore access (MS-FSA
     * Open.HasRestoreAccess): an open made with FILE_OPEN_FOR_BACKUP_INTENT.
     * chimera has no privilege model to check beyond the intent. */
    if ((ctl == SMB2_FSCTL_SET_OBJECT_ID || ctl == SMB2_FSCTL_DELETE_OBJECT_ID) &&
        !(open_file->flags & CHIMERA_SMB_OPEN_FILE_BACKUP_INTENT)) {
        chimera_smb_oid_finish(request, SMB2_STATUS_ACCESS_DENIED);
        return;
    }

    if (!open_file->handle ||
        !(open_file->handle->vfs_module->capabilities & CHIMERA_VFS_CAP_XATTR)) {
        if (ctl == SMB2_FSCTL_CREATE_OR_GET_OBJECT_ID) {
            chimera_smb_oid_generate(request, open_file);
            chimera_smb_oid_finish(request, SMB2_STATUS_SUCCESS);
        } else {
            chimera_smb_oid_finish(request, SMB2_STATUS_INVALID_DEVICE_REQUEST);
        }
        return;
    }

    memset(request->ioctl.oid_buffer, 0, CHIMERA_SMB_OID_SIZE);
    {
        struct chimera_vfs_compound *sequence = chimera_vfs_compound_alloc(request->compound->thread->vfs_thread, &
                                                                           request->session_handle->session->cred);
        request->vfs_compound = sequence;
        chimera_vfs_compound_add_puthandle(sequence, open_file->handle, CHIMERA_VFS_OPEN_PATH);
        chimera_vfs_compound_add_getxattr(sequence, CHIMERA_SMB_OID_XATTR, CHIMERA_SMB_OID_XATTR_LEN,
                                          CHIMERA_SMB_OID_SIZE);
        chimera_frontend_compound_submit(sequence, chimera_smb_oid_current_cb, request);
    }
} /* chimera_smb_ioctl_object_id */
