// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include "smb_internal.h"
#include "smb_compound.h"
#include "common/compound_retry.h"
#include "smb_procs.h"
#include "smb_common/smb2.h"
#include "vfs/vfs_compound.h"

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


/* Native batches and standalone requests use the same replayable builder.
 * All discovered opens belong to the compound. Callouts only change request
 * scratch or append operations; no handle transfer, publication or response
 * occurs before accepted finish. */
static void smb_oid_store(
    struct chimera_vfs_compound *,
    struct smb_vfs_command *);

static unsigned int
smb_oid_vfs_status(enum chimera_vfs_error error)
{
    switch (error) {
        case CHIMERA_VFS_OK: return SMB2_STATUS_SUCCESS;
        case CHIMERA_VFS_ENOTSUP: return SMB2_STATUS_INVALID_DEVICE_REQUEST;
        case CHIMERA_VFS_EACCES:
        case CHIMERA_VFS_EPERM: return SMB2_STATUS_ACCESS_DENIED;
        default: return SMB2_STATUS_INTERNAL_ERROR;
    } /* switch */
} /* smb_oid_vfs_status */

static void
smb_oid_fail(
    struct smb_vfs_command *command,
    enum chimera_vfs_error *error,
    unsigned int            status)
{
    command->status = status;
    *error          = CHIMERA_VFS_EINVAL;
} /* smb_oid_fail */

static void
smb_oid_index_name(struct chimera_smb_request *request)
{
    char *p = request->ioctl.oid_index_name;

    p += sprintf(p, "%s", CHIMERA_SMB_OID_INDEX_PREFIX);
    for (int i = 0; i < 16; i++) {
        p += sprintf(p, "%02x", request->ioctl.oid_buffer[i]);
    }
} /* smb_oid_index_name */

static void
smb_oid_generate(struct smb_vfs_command *command)
{
    uint8_t *b = command->request->ioctl.oid_buffer;

    memset(b, 0, CHIMERA_SMB_OID_SIZE);
    if (command->handle) {
        memcpy(b, &command->handle->fh_hash, 8);
        memcpy(b + 8, &command->handle->fh_hash, 8);
        b[8] ^= 0xa5;
        memcpy(b + 16, command->request->compound->thread->shared->guid, 16);
        memcpy(b + 32, b, 16);
    }
} /* smb_oid_generate */

static void
smb_oid_callback(
    struct chimera_vfs_compound       *compound,
    int                                op,
    chimera_vfs_compound_op_callback_t complete,
    struct smb_vfs_command            *command)
{
    if (op >= 0) {
        chimera_vfs_compound_set_op_callbacks(compound, op, NULL, complete, command);
    }
} /* smb_oid_callback */

/* Preserve the existing best-effort index/birth-ID repair policy for operation
 * errors. A rejected compound finish is never normalized here. */
static void
smb_oid_optional(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    (void) compound; (void) index; (void) private_data;
    *status = CHIMERA_VFS_OK;
} /* smb_oid_optional */

static void
smb_oid_update_root(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct smb_vfs_command     *command = private_data;
    struct chimera_smb_request *request = command->request;
    int                         next;

    (void) index;
    if (*status != CHIMERA_VFS_OK) {
        *status = CHIMERA_VFS_OK;
        return;
    }
    if (request->ioctl.ctl_code == SMB2_FSCTL_DELETE_OBJECT_ID) {
        next = chimera_vfs_compound_add_removexattr(compound, request->ioctl.oid_index_name,
                                                    strlen(request->ioctl.oid_index_name));
    } else {
        next = chimera_vfs_compound_add_setxattr(compound, 0, request->ioctl.oid_index_name,
                                                 strlen(request->ioctl.oid_index_name),
                                                 command->handle->fh, command->handle->fh_len);
    }
    smb_oid_callback(compound, next, smb_oid_optional, command);
} /* smb_oid_update_root */

static void
smb_oid_open_root(
    struct chimera_vfs_compound       *compound,
    struct smb_vfs_command            *command,
    chimera_vfs_compound_op_callback_t complete)
{
    struct chimera_smb_request *request = command->request;

    smb_oid_index_name(request);
    chimera_vfs_compound_add_putfh(compound, request->tree->fh, request->tree->fh_len);
    int                         next = chimera_vfs_compound_add_open_current(compound,
                                                                             CHIMERA_VFS_OPEN_PATH |
                                                                             CHIMERA_VFS_OPEN_INFERRED |
                                                                             CHIMERA_VFS_OPEN_DIRECTORY, 0);
    smb_oid_callback(compound, next, complete, command);
} /* smb_oid_open_root */

static void
smb_oid_stored(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct smb_vfs_command *command = private_data;

    (void) index;
    if (*status == CHIMERA_VFS_OK && command->request->ioctl.ctl_code != SMB2_FSCTL_SET_OBJECT_ID_EXTENDED) {
        smb_oid_open_root(compound, command, smb_oid_update_root);
    }
} /* smb_oid_stored */

static void
smb_oid_store(
    struct chimera_vfs_compound *compound,
    struct smb_vfs_command      *command)
{
    struct chimera_smb_request *request = command->request;
    int                         next    = chimera_vfs_compound_add_setxattr(compound, 0, CHIMERA_SMB_OID_XATTR,
                                                                            CHIMERA_SMB_OID_XATTR_LEN, request->ioctl.
                                                                            oid_buffer, CHIMERA_SMB_OID_SIZE);

    chimera_vfs_compound_op_set_handle(compound, next, command->handle);
    smb_oid_callback(compound, next, smb_oid_stored, command);
} /* smb_oid_store */

static void
smb_oid_other_id(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct smb_vfs_command               *command = private_data;
    const struct chimera_vfs_compound_op *op      = chimera_vfs_compound_op(compound, index);

    if (*status == CHIMERA_VFS_OK && op->buffer_len >= 16 &&
        !memcmp(op->buffer, command->request->ioctl.oid_buffer, 16)) {
        smb_oid_fail(command, status, SMB2_STATUS_DUPLICATE_NAME);
        return;
    }
    *status = CHIMERA_VFS_OK;
    smb_oid_store(compound, command);
} /* smb_oid_other_id */

static void
smb_oid_other_open(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct smb_vfs_command *command = private_data;

    (void) index;
    if (*status != CHIMERA_VFS_OK) {
        /* An index whose file is gone is stale. */
        *status = CHIMERA_VFS_OK;
        smb_oid_store(compound, command);
        return;
    }
    int next = chimera_vfs_compound_add_getxattr(compound, CHIMERA_SMB_OID_XATTR,
                                                 CHIMERA_SMB_OID_XATTR_LEN, CHIMERA_SMB_OID_SIZE);
    smb_oid_callback(compound, next, smb_oid_other_id, command);
} /* smb_oid_other_open */

static void
smb_oid_index_get(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct smb_vfs_command               *command = private_data;
    struct chimera_smb_request           *request = command->request;
    const struct chimera_vfs_compound_op *op      = chimera_vfs_compound_op(compound, index);
    uint32_t                              length  = op->buffer_len;

    if (*status != CHIMERA_VFS_OK || !length || length > sizeof(request->ioctl.oid_fh) ||
        (length == command->handle->fh_len && !memcmp(op->buffer, command->handle->fh, length))) {
        *status = CHIMERA_VFS_OK;
        smb_oid_store(compound, command);
        return;
    }
    memcpy(request->ioctl.oid_fh, op->buffer, length);
    chimera_vfs_compound_add_putfh(compound, request->ioctl.oid_fh, length);
    int next = chimera_vfs_compound_add_open_current(compound, CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_INFERRED, 0);
    smb_oid_callback(compound, next, smb_oid_other_open, command);
} /* smb_oid_index_get */

static void
smb_oid_unique_root(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct smb_vfs_command     *command = private_data;
    struct chimera_smb_request *request = command->request;

    (void) index;
    if (*status != CHIMERA_VFS_OK) {
        *status = CHIMERA_VFS_OK;
        smb_oid_store(compound, command);
        return;
    }
    int                         next = chimera_vfs_compound_add_getxattr(compound, request->ioctl.oid_index_name,
                                                                         strlen(request->ioctl.oid_index_name), sizeof(
                                                                             request->ioctl.oid_fh));
    smb_oid_callback(compound, next, smb_oid_index_get, command);
} /* smb_oid_unique_root */

static void
smb_oid_current(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct smb_vfs_command               *command = private_data;
    struct chimera_smb_request           *request = command->request;
    const struct chimera_vfs_compound_op *op      = chimera_vfs_compound_op(compound, index);
    bool                                  have    = *status == CHIMERA_VFS_OK && op->buffer_len == CHIMERA_SMB_OID_SIZE;

    if (*status != CHIMERA_VFS_OK && *status != CHIMERA_VFS_ENODATA) {
        return;
    }
    *status = CHIMERA_VFS_OK;
    if (have) {
        memcpy(request->ioctl.oid_buffer, op->buffer, CHIMERA_SMB_OID_SIZE);
    }
    switch (request->ioctl.ctl_code) {
        case SMB2_FSCTL_GET_OBJECT_ID:
            if (!have) {
                smb_oid_fail(command, status, SMB2_STATUS_OBJECTID_NOT_FOUND);
            }
            return;
        case SMB2_FSCTL_SET_OBJECT_ID:
            if (have) {
                smb_oid_fail(command, status, SMB2_STATUS_OBJECT_NAME_COLLISION);
                return;
            }
            memcpy(request->ioctl.oid_buffer, request->ioctl.oid_in, CHIMERA_SMB_OID_SIZE);
            smb_oid_open_root(compound, command, smb_oid_unique_root);
            return;
        case SMB2_FSCTL_SET_OBJECT_ID_EXTENDED:
            if (!have) {
                smb_oid_fail(command, status, SMB2_STATUS_OBJECTID_NOT_FOUND);
                return;
            }
            memcpy(request->ioctl.oid_buffer + 16, request->ioctl.oid_in, 48);
            smb_oid_store(compound, command);
            return;
        case SMB2_FSCTL_DELETE_OBJECT_ID:
            if (have) {
                int next = chimera_vfs_compound_add_removexattr(compound, CHIMERA_SMB_OID_XATTR,
                                                                CHIMERA_SMB_OID_XATTR_LEN);
                chimera_vfs_compound_op_set_handle(compound, next, command->handle);
                smb_oid_callback(compound, next, smb_oid_stored, command);
            }
            return;
        case SMB2_FSCTL_CREATE_OR_GET_OBJECT_ID:
            if (!have) {
                smb_oid_generate(command);
                smb_oid_store(compound, command);
            } else if (!memcmp(request->ioctl.oid_buffer + 16, chimera_smb_oid_zero, 16) &&
                       !memcmp(request->ioctl.oid_buffer + 32, chimera_smb_oid_zero, 16)) {
                memcpy(request->ioctl.oid_buffer + 16, request->compound->thread->shared->guid, 16);
                memcpy(request->ioctl.oid_buffer + 32, request->ioctl.oid_buffer, 16);
                memset(request->ioctl.oid_buffer + 48, 0, 16);
                int next = chimera_vfs_compound_add_setxattr(compound, 0, CHIMERA_SMB_OID_XATTR,
                                                             CHIMERA_SMB_OID_XATTR_LEN, request->ioctl.oid_buffer,
                                                             CHIMERA_SMB_OID_SIZE);
                chimera_vfs_compound_op_set_handle(compound, next, command->handle);
                smb_oid_callback(compound, next, smb_oid_optional, command);
            }
            return;
    } /* switch */
} /* smb_oid_current */

static void
smb_oid_prepare(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct smb_vfs_command     *command = private_data;
    struct chimera_smb_request *request = command->request;
    uint32_t                    ctl     = request->ioctl.ctl_code;
    uint32_t                    flags   = command->state ? command->state->flags : command->open->flags;

    (void) index;
    command->status = SMB2_STATUS_SUCCESS;
    memset(request->ioctl.oid_buffer, 0, CHIMERA_SMB_OID_SIZE);
    if (ctl == SMB2_FSCTL_CREATE_OR_GET_OBJECT_ID && command->state) {
        if (command->state->channel_sequence_valid &&
            (uint16_t) (request->channel_sequence - command->state->channel_sequence) >= 0x8000) {
            smb_oid_fail(command, status, SMB2_STATUS_FILE_NOT_AVAILABLE);
            return;
        }
        command->state->channel_sequence       = request->channel_sequence;
        command->state->channel_sequence_valid = command->state->sequence_dirty = 1;
    }
    if ((ctl == SMB2_FSCTL_GET_OBJECT_ID || ctl == SMB2_FSCTL_CREATE_OR_GET_OBJECT_ID) &&
        request->ioctl.max_output_response < CHIMERA_SMB_OID_SIZE) {
        smb_oid_fail(command, status, SMB2_STATUS_INVALID_PARAMETER);
        return;
    }
    if ((ctl == SMB2_FSCTL_SET_OBJECT_ID_EXTENDED &&
         !(command->open->granted_access & (SMB2_FILE_WRITE_DATA | SMB2_FILE_WRITE_ATTRIBUTES))) ||
        ((ctl == SMB2_FSCTL_SET_OBJECT_ID || ctl == SMB2_FSCTL_DELETE_OBJECT_ID) &&
         !(flags & CHIMERA_SMB_OPEN_FILE_BACKUP_INTENT))) {
        smb_oid_fail(command, status, SMB2_STATUS_ACCESS_DENIED);
        return;
    }
    if (!command->handle || !(command->handle->vfs_module->capabilities & CHIMERA_VFS_CAP_XATTR)) {
        if (ctl == SMB2_FSCTL_CREATE_OR_GET_OBJECT_ID) {
            smb_oid_generate(command);
        } else {
            smb_oid_fail(command, status, SMB2_STATUS_INVALID_DEVICE_REQUEST);
        }
        return;
    }
    int next = chimera_vfs_compound_add_getxattr(compound, CHIMERA_SMB_OID_XATTR,
                                                 CHIMERA_SMB_OID_XATTR_LEN, CHIMERA_SMB_OID_SIZE);
    chimera_vfs_compound_op_set_handle(compound, next, command->handle);
    smb_oid_callback(compound, next, smb_oid_current, command);
} /* smb_oid_prepare */

static int
smb_oid_build(
    struct chimera_vfs_compound *compound,
    struct smb_vfs_command      *command)
{
    (void) command;
    return chimera_vfs_compound_add_checkpoint(compound);
} /* smb_oid_build */

static int
smb_oid_eligible(struct chimera_smb_request *request)
{
    (void) request;
    return 1;
} /* smb_oid_eligible */

static struct chimera_smb_file_id
smb_oid_file_id(struct chimera_smb_request *request)
{
    return request->ioctl.file_id;
} /* smb_oid_file_id */

const struct smb_vfs_command_ops chimera_smb_object_id_compound_ops = {
    .file_id   = smb_oid_file_id,
    .eligible  = smb_oid_eligible,
    .map_error = smb_oid_vfs_status,
    .build     = smb_oid_build,
    .prepare   = smb_oid_prepare,
};

static void
smb_oid_done(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct smb_vfs_command     *command = private_data;
    struct chimera_smb_request *request = command->request;
    unsigned int                status  = command->status;

    if (chimera_vfs_compound_finish_status(compound) != CHIMERA_VFS_OK || status == SMB2_STATUS_SUCCESS) {
        status = smb_oid_vfs_status(chimera_vfs_compound_status(compound));
    }
    chimera_vfs_compound_free(compound);
    request->vfs_compound = NULL;
    chimera_smb_open_file_release(request, command->open);
    free(command);
    chimera_smb_complete_request(request, status);
} /* smb_oid_done */

void
chimera_smb_ioctl_object_id(struct chimera_smb_request *request)
{
    struct chimera_smb_open_file *open = chimera_smb_open_file_resolve(request, &request->ioctl.file_id);

    if (!open) {
        chimera_smb_complete_request(request, SMB2_STATUS_FILE_CLOSED);
        return;
    }
    struct smb_vfs_command       *command = calloc(1, sizeof(*command));
    if (!command) {
        chimera_smb_open_file_release(request, open);
        chimera_smb_complete_request(request, SMB2_STATUS_INSUFFICIENT_RESOURCES);
        return;
    }
    command->request = request;
    command->open    = open;
    command->handle  = open->handle;
    struct chimera_vfs_compound *compound = chimera_vfs_compound_alloc(request->compound->thread->vfs_thread,
                                                                       &request->session_handle->session->cred);
    request->vfs_compound = compound;
    int                          op = smb_oid_build(compound, command);
    chimera_vfs_compound_set_op_callbacks(compound, op, smb_oid_prepare, NULL, command);
    chimera_frontend_compound_submit(compound, smb_oid_done, command);
} /* chimera_smb_ioctl_object_id */
