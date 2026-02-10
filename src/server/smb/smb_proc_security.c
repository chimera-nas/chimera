// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * SMB2 SET_INFO / QUERY_INFO handlers for SMB2_INFO_SECURITY (0x03).
 *
 * Implements "modefromsid" semantics: Unix mode/uid/gid are encoded in
 * special SIDs within an NT Security Descriptor.
 *
 *   S-1-5-88-1-<uid>   Unix UID
 *   S-1-5-88-2-<gid>   Unix GID
 *   S-1-5-88-3-<mode>  Unix permission bits
 */

#include "smb_internal.h"
#include "smb_procs.h"
#include "vfs/vfs.h"

/* Security information flags (addl_info) */
#define OWNER_SECURITY_INFORMATION 0x00000001
#define GROUP_SECURITY_INFORMATION 0x00000002
#define DACL_SECURITY_INFORMATION  0x00000004

/* Security descriptor control flags */
#define SE_SELF_RELATIVE           0x8000
#define SE_DACL_PRESENT            0x0004

/* Size of a SID with 3 sub-authorities: S-1-5-88-X-Y */
#define SID_UNIX_SIZE              20 /* 1+1+6+3*4 */

/* Size of an ACE containing a 3-sub-authority SID */
#define ACE_UNIX_SIZE              28 /* 1+1+2+4 + SID_UNIX_SIZE */

/* Size of an ACL containing one ACE */
#define ACL_UNIX_SIZE              36 /* 2+2+2+2 + ACE_UNIX_SIZE */

/* Size of the security descriptor header */
#define SD_HEADER_SIZE             20

/*
 * Check whether a SID at the given buffer position is S-1-5-88-<kind>-<value>
 * and extract <value>.  Returns 0 on match, -1 otherwise.
 */
static int
parse_unix_sid(
    const uint8_t *buf,
    uint32_t       len,
    uint32_t       kind,
    uint32_t      *value)
{
    if (len < SID_UNIX_SIZE) {
        return -1;
    }

    /* revision must be 1, sub_authority_count must be 3 */
    if (buf[0] != 1 || buf[1] != 3) {
        return -1;
    }

    /* authority must be {0,0,0,0,0,5} (NT Authority) */
    if (buf[2] != 0 || buf[3] != 0 || buf[4] != 0 ||
        buf[5] != 0 || buf[6] != 0 || buf[7] != 5) {
        return -1;
    }

    /* sub_authority[0] must be 88 (little-endian) */
    uint32_t sa0 = buf[8]  | (buf[9] << 8) | (buf[10] << 16) | (buf[11] << 24);
    uint32_t sa1 = buf[12] | (buf[13] << 8) | (buf[14] << 16) | (buf[15] << 24);
    uint32_t sa2 = buf[16] | (buf[17] << 8) | (buf[18] << 16) | (buf[19] << 24);

    if (sa0 != 88 || sa1 != kind) {
        return -1;
    }

    *value = sa2;
    return 0;
} /* parse_unix_sid */

/*
 * Write a SID S-1-5-88-<kind>-<value> into buf (must have SID_UNIX_SIZE bytes).
 */
static void
write_unix_sid(
    uint8_t *buf,
    uint32_t kind,
    uint32_t value)
{
    buf[0] = 1;  /* revision */
    buf[1] = 3;  /* sub_authority_count */
    /* authority: NT Authority {0,0,0,0,0,5} */
    buf[2] = 0; buf[3] = 0; buf[4] = 0;
    buf[5] = 0; buf[6] = 0; buf[7] = 5;
    /* sub_authority[0] = 88 (little-endian) */
    buf[8] = 88; buf[9]  = 0; buf[10] = 0; buf[11] = 0;
    /* sub_authority[1] = kind */
    buf[12] = kind & 0xff; buf[13] = (kind >> 8) & 0xff;
    buf[14] = (kind >> 16) & 0xff; buf[15] = (kind >> 24) & 0xff;
    /* sub_authority[2] = value */
    buf[16] = value & 0xff; buf[17] = (value >> 8) & 0xff;
    buf[18] = (value >> 16) & 0xff; buf[19] = (value >> 24) & 0xff;
} /* write_unix_sid */

/* ------------------------------------------------------------------ */
/* SET_INFO handler for SMB2_INFO_SECURITY                            */
/* ------------------------------------------------------------------ */

static void
chimera_smb_set_security_setattr_callback(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *set_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct chimera_smb_request *request = private_data;

    chimera_smb_open_file_release(request, request->set_info.open_file);
    chimera_smb_complete_request(request, error_code ? SMB2_STATUS_INTERNAL_ERROR : SMB2_STATUS_SUCCESS);
} /* chimera_smb_set_security_setattr_callback */

void
chimera_smb_parse_sd_to_attrs(
    const uint8_t            *sd_buf,
    uint32_t                  sd_len,
    struct chimera_vfs_attrs *attrs)
{
    uint32_t value;

    if (sd_len < SD_HEADER_SIZE) {
        return;
    }

    /* Parse security descriptor header (self-relative format) */
    uint32_t offset_owner = sd_buf[4]  | (sd_buf[5] << 8) | (sd_buf[6] << 16) | (sd_buf[7] << 24);
    uint32_t offset_group = sd_buf[8]  | (sd_buf[9] << 8) | (sd_buf[10] << 16) | (sd_buf[11] << 24);
    uint32_t offset_dacl  = sd_buf[16] | (sd_buf[17] << 8) | (sd_buf[18] << 16) | (sd_buf[19] << 24);

    /* Owner SID → uid */
    if (offset_owner && offset_owner + SID_UNIX_SIZE <= sd_len) {
        if (parse_unix_sid(sd_buf + offset_owner, sd_len - offset_owner, 1, &value) == 0) {
            attrs->va_uid       = value;
            attrs->va_set_mask |= CHIMERA_VFS_ATTR_UID;
        }
    }

    /* Group SID → gid */
    if (offset_group && offset_group + SID_UNIX_SIZE <= sd_len) {
        if (parse_unix_sid(sd_buf + offset_group, sd_len - offset_group, 2, &value) == 0) {
            attrs->va_gid       = value;
            attrs->va_set_mask |= CHIMERA_VFS_ATTR_GID;
        }
    }

    /* DACL → scan ACEs for mode SID */
    if (offset_dacl && offset_dacl + 8 <= sd_len) {
        const uint8_t *acl_buf   = sd_buf + offset_dacl;
        uint16_t       acl_size  = acl_buf[2] | (acl_buf[3] << 8);
        uint16_t       ace_count = acl_buf[4] | (acl_buf[5] << 8);
        uint32_t       pos       = 8; /* skip ACL header */

        for (uint16_t i = 0; i < ace_count && pos + 8 <= acl_size &&
             offset_dacl + pos + 8 <= sd_len; i++) {
            uint16_t ace_size = acl_buf[pos + 2] | (acl_buf[pos + 3] << 8);
            /* ACE header is 4 bytes, then 4 bytes access mask, then SID */
            uint32_t sid_offset = pos + 8;

            if (offset_dacl + sid_offset + SID_UNIX_SIZE <= sd_len) {
                if (parse_unix_sid(acl_buf + sid_offset,
                                   sd_len - offset_dacl - sid_offset, 3, &value) == 0) {
                    attrs->va_mode      = value;
                    attrs->va_set_mask |= CHIMERA_VFS_ATTR_MODE;
                    break;
                }
            }
            pos += ace_size;
        }
    }
} /* chimera_smb_parse_sd_to_attrs */

void
chimera_smb_set_security(struct chimera_smb_request *request)
{
    struct chimera_vfs_attrs *vfs_attrs = &request->set_info.vfs_attrs;
    const uint8_t            *sd_buf;
    uint32_t                  sd_len;

    vfs_attrs->va_req_mask = 0;
    vfs_attrs->va_set_mask = 0;

    /*
     * The raw security descriptor is in the request buffer.
     * We saved the cursor position and buffer length during parse.
     */
    sd_buf = request->set_info.sec_buf;
    sd_len = request->set_info.sec_buf_len;

    chimera_smb_parse_sd_to_attrs(sd_buf, sd_len, vfs_attrs);

    if (vfs_attrs->va_set_mask == 0) {
        /* Nothing to change */
        chimera_smb_open_file_release(request, request->set_info.open_file);
        chimera_smb_complete_request(request, SMB2_STATUS_SUCCESS);
        return;
    }

    chimera_vfs_setattr(
        request->compound->thread->vfs_thread,
        &request->session_handle->session->cred,
        request->set_info.open_file->handle,
        vfs_attrs,
        0,
        0,
        chimera_smb_set_security_setattr_callback,
        request);
} /* chimera_smb_set_security */ /* chimera_smb_set_security */

/* ------------------------------------------------------------------ */
/* QUERY_INFO handler for SMB2_INFO_SECURITY                          */
/* ------------------------------------------------------------------ */

static void
chimera_smb_query_security_getattr_callback(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_smb_request *request = private_data;

    if (error_code) {
        chimera_smb_open_file_release(request, request->query_info.open_file);
        chimera_smb_complete_request(request, SMB2_STATUS_INTERNAL_ERROR);
        return;
    }

    /*
     * Store uid/gid/mode in the request so the reply function can build
     * the security descriptor.
     */
    request->query_info.r_attrs.smb_ino                       = attr->va_mode; /* borrow ino for mode */
    request->query_info.r_fs_attrs.smb_total_allocation_units =
        attr->va_uid;                                                          /* borrow for uid */
    request->query_info.r_fs_attrs.smb_caller_available_allocation_units =
        attr->va_gid;                                                          /* borrow for gid */

    chimera_smb_open_file_release(request, request->query_info.open_file);
    chimera_smb_complete_request(request, SMB2_STATUS_SUCCESS);
} /* chimera_smb_query_security_getattr_callback */

void
chimera_smb_query_security(struct chimera_smb_request *request)
{
    chimera_vfs_getattr(
        request->compound->thread->vfs_thread,
        &request->session_handle->session->cred,
        request->query_info.open_file->handle,
        CHIMERA_VFS_ATTR_MASK_STAT,
        chimera_smb_query_security_getattr_callback,
        request);
} /* chimera_smb_query_security */

void
chimera_smb_query_security_reply(
    struct evpl_iovec_cursor   *reply_cursor,
    struct chimera_smb_request *request)
{
    uint32_t addl_info = request->query_info.addl_info;
    uint32_t uid       = (uint32_t) request->query_info.r_fs_attrs.smb_total_allocation_units;
    uint32_t gid       = (uint32_t) request->query_info.r_fs_attrs.smb_caller_available_allocation_units;
    uint32_t mode      = (uint32_t) request->query_info.r_attrs.smb_ino;
    uint8_t  sd[SD_HEADER_SIZE + SID_UNIX_SIZE * 2 + ACL_UNIX_SIZE];
    uint32_t sd_len;
    uint32_t offset;
    int      has_owner = !!(addl_info & OWNER_SECURITY_INFORMATION);
    int      has_group = !!(addl_info & GROUP_SECURITY_INFORMATION);
    int      has_dacl  = !!(addl_info & DACL_SECURITY_INFORMATION);
    uint16_t control   = SE_SELF_RELATIVE;

    memset(sd, 0, sizeof(sd));

    if (has_dacl) {
        control |= SE_DACL_PRESENT;
    }

    /* Build security descriptor: header, then DACL, owner, group */
    offset = SD_HEADER_SIZE;

    uint32_t dacl_offset  = 0;
    uint32_t owner_offset = 0;
    uint32_t group_offset = 0;

    if (has_dacl) {
        dacl_offset = offset;

        /* ACL header */
        sd[offset + 0] = 2; /* revision */
        sd[offset + 1] = 0; /* reserved */
        /* acl size (little-endian) */
        sd[offset + 2] = ACL_UNIX_SIZE & 0xff;
        sd[offset + 3] = (ACL_UNIX_SIZE >> 8) & 0xff;
        /* ace count */
        sd[offset + 4] = 1;
        sd[offset + 5] = 0;
        /* reserved */
        sd[offset + 6] = 0;
        sd[offset + 7] = 0;

        /* ACE header */
        sd[offset + 8]  = 0; /* ACCESS_ALLOWED_ACE_TYPE */
        sd[offset + 9]  = 0; /* flags */
        sd[offset + 10] = ACE_UNIX_SIZE & 0xff;
        sd[offset + 11] = (ACE_UNIX_SIZE >> 8) & 0xff;
        /* access mask (GENERIC_ALL) */
        sd[offset + 12] = 0;
        sd[offset + 13] = 0;
        sd[offset + 14] = 0;
        sd[offset + 15] = 0x10;

        /* SID S-1-5-88-3-<mode> */
        write_unix_sid(&sd[offset + 16], 3, mode & 07777);

        offset += ACL_UNIX_SIZE;
    }

    if (has_owner) {
        owner_offset = offset;
        write_unix_sid(&sd[offset], 1, uid);
        offset += SID_UNIX_SIZE;
    }

    if (has_group) {
        group_offset = offset;
        write_unix_sid(&sd[offset], 2, gid);
        offset += SID_UNIX_SIZE;
    }

    sd_len = offset;

    /* Fill in the header */
    sd[0] = 1; /* revision */
    sd[1] = 0; /* reserved */
    sd[2] = control & 0xff;
    sd[3] = (control >> 8) & 0xff;
    /* offset_owner */
    sd[4] = owner_offset & 0xff; sd[5] = (owner_offset >> 8) & 0xff;
    sd[6] = (owner_offset >> 16) & 0xff; sd[7] = (owner_offset >> 24) & 0xff;
    /* offset_group */
    sd[8]  = group_offset & 0xff; sd[9] = (group_offset >> 8) & 0xff;
    sd[10] = (group_offset >> 16) & 0xff; sd[11] = (group_offset >> 24) & 0xff;
    /* offset_sacl = 0 */
    sd[12] = 0; sd[13] = 0; sd[14] = 0; sd[15] = 0;
    /* offset_dacl */
    sd[16] = dacl_offset & 0xff; sd[17] = (dacl_offset >> 8) & 0xff;
    sd[18] = (dacl_offset >> 16) & 0xff; sd[19] = (dacl_offset >> 24) & 0xff;

    evpl_iovec_cursor_append_uint16(reply_cursor, SMB2_QUERY_INFO_REPLY_SIZE);
    evpl_iovec_cursor_append_uint16(reply_cursor, 64 + 8);
    evpl_iovec_cursor_append_uint32(reply_cursor, sd_len);
    evpl_iovec_cursor_append_blob_unaligned(reply_cursor, sd, sd_len);
} /* chimera_smb_query_security_reply */
