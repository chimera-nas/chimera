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
#define SE_DACL_AUTO_INHERIT_REQ   0x0100
#define SE_DACL_AUTO_INHERITED     0x0400
#define SE_DACL_PROTECTED          0x1000

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
/* General SID <-> string and NT ACE <-> canonical ACE translation    */
/* ------------------------------------------------------------------ */

/* NT ACE header flag bits (differ from the canonical/NFSv4 layout). */
#define NT_ACE_OBJECT_INHERIT    0x01
#define NT_ACE_CONTAINER_INHERIT 0x02
#define NT_ACE_NO_PROPAGATE      0x04
#define NT_ACE_INHERIT_ONLY      0x08
#define NT_ACE_INHERITED         0x10
#define NT_ACE_SUCCESSFUL_ACCESS 0x40
#define NT_ACE_FAILED_ACCESS     0x80

/* NT generic access bits and their specific-rights expansions. */
#define NT_GENERIC_READ          0x80000000
#define NT_GENERIC_WRITE         0x40000000
#define NT_GENERIC_EXECUTE       0x20000000
#define NT_GENERIC_ALL           0x10000000
/* FILE_GENERIC_{READ,WRITE,EXECUTE}: STANDARD_RIGHTS_* | the file-specific
 * read/write/execute rights | SYNCHRONIZE.  Each includes its EA right
 * (READ_EA 0x8 / WRITE_EA 0x10), which earlier values omitted. */
#define ACE4_GENERIC_READ        0x00120089
#define ACE4_GENERIC_WRITE       0x00120116
#define ACE4_GENERIC_EXECUTE     0x001200a0

/*
 * Format a binary SID into "S-<rev>-<authority>-<sub>..." text.  Returns the
 * number of bytes consumed from `buf`, or -1 on a malformed/truncated SID.
 */
static int
sid_bin_to_str(
    const uint8_t *buf,
    uint32_t       len,
    char          *out,
    int            outlen)
{
    uint8_t  rev, count;
    uint64_t authority = 0;
    int      pos;

    if (len < 8) {
        return -1;
    }
    rev   = buf[0];
    count = buf[1];
    if (8 + (uint32_t) count * 4 > len) {
        return -1;
    }
    for (int i = 0; i < 6; i++) {
        authority = (authority << 8) | buf[2 + i];
    }

    pos = snprintf(out, outlen, "S-%u-%llu", rev, (unsigned long long) authority);
    for (int i = 0; i < count; i++) {
        uint32_t sa = buf[8 + i * 4] | (buf[8 + i * 4 + 1] << 8) |
            (buf[8 + i * 4 + 2] << 16) | ((uint32_t) buf[8 + i * 4 + 3] << 24);

        if (pos < 0 || pos >= outlen) {
            return -1;
        }
        pos += snprintf(out + pos, outlen - pos, "-%u", sa);
    }
    if (pos < 0 || pos >= outlen) {
        return -1;
    }
    return 8 + count * 4;
} /* sid_bin_to_str */

/*
 * Parse "S-<rev>-<authority>-<sub>..." into a binary SID.  Returns the binary
 * length written, or -1 on malformed input / insufficient capacity.
 */
static int
sid_str_to_bin(
    const char *str,
    uint8_t    *out,
    int         outcap)
{
    const char *p = str;
    uint64_t    authority;
    uint8_t     count = 0;
    char       *end;

    if (str[0] != 'S' && str[0] != 's') {
        return -1;
    }
    p++;
    if (*p != '-') {
        return -1;
    }
    p++;
    out[0] = (uint8_t) strtoul(p, &end, 10); /* revision */
    if (end == p || *end != '-') {
        return -1;
    }
    p = end + 1;

    authority = strtoull(p, &end, 10);
    if (end == p) {
        return -1;
    }
    for (int i = 0; i < 6; i++) {
        out[2 + i] = (uint8_t) ((authority >> (8 * (5 - i))) & 0xff);
    }
    p = end;

    while (*p == '-') {
        uint32_t sa;

        p++;
        sa = (uint32_t) strtoul(p, &end, 10);
        if (end == p) {
            return -1;
        }
        if (8 + (count + 1) * 4 > outcap) {
            return -1;
        }
        out[8 + count * 4]     = sa & 0xff;
        out[8 + count * 4 + 1] = (sa >> 8) & 0xff;
        out[8 + count * 4 + 2] = (sa >> 16) & 0xff;
        out[8 + count * 4 + 3] = (sa >> 24) & 0xff;
        count++;
        p = end;
    }

    out[1] = count;
    return 8 + count * 4;
} /* sid_str_to_bin */

static uint16_t
nt_flags_to_canon(uint8_t f)
{
    uint16_t c = f & 0x0f; /* the four inheritance bits share the layout */

    if (f & NT_ACE_INHERITED) {
        c |= CHIMERA_ACE_FLAG_INHERITED;
    }
    if (f & NT_ACE_SUCCESSFUL_ACCESS) {
        c |= CHIMERA_ACE_FLAG_SUCCESSFUL_ACCESS;
    }
    if (f & NT_ACE_FAILED_ACCESS) {
        c |= CHIMERA_ACE_FLAG_FAILED_ACCESS;
    }
    return c;
} /* nt_flags_to_canon */

static uint8_t
canon_flags_to_nt(uint16_t c)
{
    uint8_t f = c & 0x0f;

    if (c & CHIMERA_ACE_FLAG_INHERITED) {
        f |= NT_ACE_INHERITED;
    }
    if (c & CHIMERA_ACE_FLAG_SUCCESSFUL_ACCESS) {
        f |= NT_ACE_SUCCESSFUL_ACCESS;
    }
    if (c & CHIMERA_ACE_FLAG_FAILED_ACCESS) {
        f |= NT_ACE_FAILED_ACCESS;
    }
    return f;
} /* canon_flags_to_nt */

/* Expand NT generic access bits into the equivalent specific rights. */
static uint32_t
expand_generic_mask(uint32_t m)
{
    uint32_t out = m & 0x01ffffff; /* specific + standard rights */

    if (m & NT_GENERIC_READ) {
        out |= ACE4_GENERIC_READ;
    }
    if (m & NT_GENERIC_WRITE) {
        out |= ACE4_GENERIC_WRITE;
    }
    if (m & NT_GENERIC_EXECUTE) {
        out |= ACE4_GENERIC_EXECUTE;
    }
    if (m & NT_GENERIC_ALL) {
        out |= CHIMERA_ACE_MASK_ALL;
    }
    return out;
} /* expand_generic_mask */

/*
 * Parse a full self-relative security descriptor into owner/group ids and a
 * canonical ACL.  Owner/group come from the SID via the idmap; the DACL is
 * translated ACE-by-ACE.  Falls back to recognising the modefromsid encoding
 * (S-1-5-88-3-<mode>) to recover a POSIX mode.  Returns 0 on success.
 */
static int
chimera_smb_sd_to_acl(
    const uint8_t            *sd_buf,
    uint32_t                  sd_len,
    struct chimera_vfs_attrs *attrs,
    struct chimera_acl       *acl,
    unsigned                  acl_max_aces)
{
    uint32_t offset_owner, offset_group, offset_dacl;
    uint32_t value;
    char     sidstr[CHIMERA_IDMAP_SID_MAX];

    if (sd_len < SD_HEADER_SIZE) {
        return -1;
    }

    offset_owner = sd_buf[4] | (sd_buf[5] << 8) | (sd_buf[6] << 16) | ((uint32_t) sd_buf[7] << 24);
    offset_group = sd_buf[8] | (sd_buf[9] << 8) | (sd_buf[10] << 16) | ((uint32_t) sd_buf[11] << 24);
    offset_dacl  = sd_buf[16] | (sd_buf[17] << 8) | (sd_buf[18] << 16) | ((uint32_t) sd_buf[19] << 24);

    /* Owner SID -> uid (modefromsid first, then general idmap). */
    if (offset_owner && offset_owner + 8 <= sd_len) {
        struct chimera_principal p;

        if (parse_unix_sid(sd_buf + offset_owner, sd_len - offset_owner, 1, &value) == 0) {
            attrs->va_uid       = value;
            attrs->va_set_mask |= CHIMERA_VFS_ATTR_UID;
        } else if (sid_bin_to_str(sd_buf + offset_owner, sd_len - offset_owner,
                                  sidstr, sizeof(sidstr)) > 0 &&
                   chimera_idmap_sid_to_principal(sidstr, &p) == 0 &&
                   p.type != CHIMERA_PRINCIPAL_SPECIAL) {
            attrs->va_uid       = p.id;
            attrs->va_set_mask |= CHIMERA_VFS_ATTR_UID;
        }
    }

    /* Group SID -> gid. */
    if (offset_group && offset_group + 8 <= sd_len) {
        struct chimera_principal p;

        if (parse_unix_sid(sd_buf + offset_group, sd_len - offset_group, 2, &value) == 0) {
            attrs->va_gid       = value;
            attrs->va_set_mask |= CHIMERA_VFS_ATTR_GID;
        } else if (sid_bin_to_str(sd_buf + offset_group, sd_len - offset_group,
                                  sidstr, sizeof(sidstr)) > 0 &&
                   chimera_idmap_sid_to_principal(sidstr, &p) == 0 &&
                   p.type != CHIMERA_PRINCIPAL_SPECIAL) {
            attrs->va_gid       = p.id;
            attrs->va_set_mask |= CHIMERA_VFS_ATTR_GID;
        }
    }

    /* DACL -> canonical ACL. */
    if (acl && offset_dacl && offset_dacl + 8 <= sd_len) {
        const uint8_t *acl_buf   = sd_buf + offset_dacl;
        uint16_t       acl_size  = acl_buf[2] | (acl_buf[3] << 8);
        uint16_t       ace_count = acl_buf[4] | (acl_buf[5] << 8);
        uint32_t       pos       = 8;
        unsigned       n         = 0;

        if (offset_dacl + acl_size > sd_len) {
            acl_size = sd_len - offset_dacl;
        }

        for (uint16_t i = 0; i < ace_count && n < acl_max_aces &&
             pos + 8 <= acl_size; i++) {
            uint8_t                  ace_type  = acl_buf[pos];
            uint8_t                  ace_flags = acl_buf[pos + 1];
            uint16_t                 ace_size  = acl_buf[pos + 2] | (acl_buf[pos + 3] << 8);
            uint32_t                 mask      = acl_buf[pos + 4] | (acl_buf[pos + 5] << 8) |
                (acl_buf[pos + 6] << 16) | ((uint32_t) acl_buf[pos + 7] << 24);
            uint32_t                 modeval;
            struct chimera_principal p;

            if (ace_size < 8 || pos + ace_size > acl_size) {
                break;
            }

            /* modefromsid mode ACE: recover POSIX mode, do not store as ACE. */
            if (parse_unix_sid(acl_buf + pos + 8, acl_size - pos - 8, 3,
                               &modeval) == 0) {
                attrs->va_mode      = modeval;
                attrs->va_set_mask |= CHIMERA_VFS_ATTR_MODE;
                pos                += ace_size;
                continue;
            }

            if (sid_bin_to_str(acl_buf + pos + 8, acl_size - pos - 8,
                               sidstr, sizeof(sidstr)) <= 0 ||
                chimera_idmap_sid_to_principal(sidstr, &p) != 0) {
                pos += ace_size;
                continue;
            }

            acl->aces[n].type        = ace_type;
            acl->aces[n].flags       = nt_flags_to_canon(ace_flags);
            acl->aces[n].access_mask = expand_generic_mask(mask);
            acl->aces[n].who         = p;
            n++;
            pos += ace_size;
        }

        uint16_t sd_control = sd_buf[2] | (sd_buf[3] << 8);

        acl->num_aces   = n;
        acl->ctrl_flags = 0;
        /* The stored DACL is in auto-inherit mode only when the client both
         * requested it (AUTO_INHERIT_REQ) and marked the descriptor as
         * auto-inherited (AUTO_INHERITED) -- REQ alone does not.  PROTECTED is
         * stored as-is. */
        if ((sd_control & SE_DACL_AUTO_INHERIT_REQ) &&
            (sd_control & SE_DACL_AUTO_INHERITED)) {
            acl->ctrl_flags |= CHIMERA_ACL_CTRL_AUTO_INHERITED;
        }
        if (sd_control & SE_DACL_PROTECTED) {
            acl->ctrl_flags |= CHIMERA_ACL_CTRL_PROTECTED;
        }
        if (n > 0) {
            attrs->va_acl       = acl;
            attrs->va_set_mask |= CHIMERA_VFS_ATTR_ACL;
        }
    }

    return 0;
} /* chimera_smb_sd_to_acl */

/*
 * Build a self-relative security descriptor from owner/group ids and a
 * canonical ACL into `out` (capacity `cap`).  Returns the SD length, or -1 if
 * it does not fit.  When `acl` is NULL only owner/group (and a modefromsid mode
 * ACE) are emitted, matching the legacy single-ACE behaviour.
 */
static int
chimera_smb_acl_to_sd(
    uint32_t                  uid,
    uint32_t                  gid,
    uint32_t                  mode,
    const struct chimera_acl *acl,
    int                       has_owner,
    int                       has_group,
    int                       has_dacl,
    uint8_t                  *out,
    uint32_t                  cap)
{
    uint32_t offset = SD_HEADER_SIZE;
    uint32_t dacl_off = 0, owner_off = 0, group_off = 0;
    uint16_t control = SE_SELF_RELATIVE;

    if (cap < SD_HEADER_SIZE) {
        return -1;
    }
    memset(out, 0, SD_HEADER_SIZE);

    if (has_dacl) {
        uint32_t acl_hdr   = offset;
        uint16_t ace_count = 0;
        uint32_t ace_pos;

        control |= SE_DACL_PRESENT;

        /* Reflect the canonical ACL control bits.  AUTO_INHERITED is set only
         * for a DACL produced by inheritance (so an explicitly-set SD round
         * trips without it); PROTECTED blocks inheritance from the parent. */
        if (acl && (acl->ctrl_flags & CHIMERA_ACL_CTRL_AUTO_INHERITED)) {
            control |= SE_DACL_AUTO_INHERITED;
        }
        if (acl && (acl->ctrl_flags & CHIMERA_ACL_CTRL_PROTECTED)) {
            control |= SE_DACL_PROTECTED;
        }

        if (offset + 8 > cap) {
            return -1;
        }
        ace_pos = offset + 8; /* after ACL header */

        if (acl && acl->num_aces) {
            for (unsigned i = 0; i < acl->num_aces; i++) {
                const struct chimera_ace *ace = &acl->aces[i];
                struct chimera_principal  who = ace->who;
                char                      sidstr[CHIMERA_IDMAP_SID_MAX];
                int                       sidlen;
                uint16_t                  ace_size;

                /* OWNER@/GROUP@ denote the object's current owner/group: emit
                 * the concrete owner/group SID rather than a special SID. */
                if (who.type == CHIMERA_PRINCIPAL_SPECIAL) {
                    if (who.special == CHIMERA_WHO_OWNER) {
                        who.type = CHIMERA_PRINCIPAL_USER;
                        who.id   = uid;
                    } else if (who.special == CHIMERA_WHO_GROUP) {
                        who.type = CHIMERA_PRINCIPAL_GROUP;
                        who.id   = gid;
                    }
                }

                if (chimera_idmap_principal_to_sid(&who, sidstr,
                                                   sizeof(sidstr)) < 0) {
                    continue;
                }
                if (ace_pos + 8 > cap) {
                    return -1;
                }
                sidlen = sid_str_to_bin(sidstr, out + ace_pos + 8, cap - ace_pos - 8);
                if (sidlen < 0) {
                    return -1;
                }
                ace_size = 8 + sidlen;

                out[ace_pos]     = (uint8_t) ace->type;
                out[ace_pos + 1] = canon_flags_to_nt(ace->flags);
                out[ace_pos + 2] = ace_size & 0xff;
                out[ace_pos + 3] = (ace_size >> 8) & 0xff;
                out[ace_pos + 4] = ace->access_mask & 0xff;
                out[ace_pos + 5] = (ace->access_mask >> 8) & 0xff;
                out[ace_pos + 6] = (ace->access_mask >> 16) & 0xff;
                out[ace_pos + 7] = (ace->access_mask >> 24) & 0xff;

                ace_pos += ace_size;
                ace_count++;
            }
        } else {
            /* No ACL: emit one modefromsid GENERIC_ALL ACE (legacy). */
            if (ace_pos + ACE_UNIX_SIZE > cap) {
                return -1;
            }
            out[ace_pos]      = 0;         /* ACCESS_ALLOWED */
            out[ace_pos + 1]  = 0;
            out[ace_pos + 2]  = ACE_UNIX_SIZE & 0xff;
            out[ace_pos + 3]  = (ACE_UNIX_SIZE >> 8) & 0xff;
            out[ace_pos + 15] = 0x10;      /* GENERIC_ALL */
            write_unix_sid(&out[ace_pos + 8], 3, mode & 07777);
            ace_pos  += ACE_UNIX_SIZE;
            ace_count = 1;
        }

        /* ACL header */
        uint16_t acl_size = ace_pos - acl_hdr;
        out[acl_hdr]     = 2; /* revision */
        out[acl_hdr + 1] = 0;
        out[acl_hdr + 2] = acl_size & 0xff;
        out[acl_hdr + 3] = (acl_size >> 8) & 0xff;
        out[acl_hdr + 4] = ace_count & 0xff;
        out[acl_hdr + 5] = (ace_count >> 8) & 0xff;
        out[acl_hdr + 6] = 0;
        out[acl_hdr + 7] = 0;

        dacl_off = acl_hdr;
        offset   = ace_pos;
    }

    if (has_owner) {
        if (offset + SID_UNIX_SIZE > cap) {
            return -1;
        }
        owner_off = offset;
        write_unix_sid(&out[offset], 1, uid);
        offset += SID_UNIX_SIZE;
    }

    if (has_group) {
        if (offset + SID_UNIX_SIZE > cap) {
            return -1;
        }
        group_off = offset;
        write_unix_sid(&out[offset], 2, gid);
        offset += SID_UNIX_SIZE;
    }

    out[0]  = 1; /* revision */
    out[1]  = 0;
    out[2]  = control & 0xff;
    out[3]  = (control >> 8) & 0xff;
    out[4]  = owner_off & 0xff; out[5] = (owner_off >> 8) & 0xff;
    out[6]  = (owner_off >> 16) & 0xff; out[7] = (owner_off >> 24) & 0xff;
    out[8]  = group_off & 0xff; out[9] = (group_off >> 8) & 0xff;
    out[10] = (group_off >> 16) & 0xff; out[11] = (group_off >> 24) & 0xff;
    out[12] = 0; out[13] = 0; out[14] = 0; out[15] = 0; /* SACL */
    out[16] = dacl_off & 0xff; out[17] = (dacl_off >> 8) & 0xff;
    out[18] = (dacl_off >> 16) & 0xff; out[19] = (dacl_off >> 24) & 0xff;

    return (int) offset;
} /* chimera_smb_acl_to_sd */

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
    unsigned int                status;

    chimera_smb_open_file_release(request, request->set_info.open_file);

    switch (error_code) {
        case CHIMERA_VFS_OK:
            status = SMB2_STATUS_SUCCESS;
            break;
        case CHIMERA_VFS_EACCES:
        case CHIMERA_VFS_EPERM:
            status = SMB2_STATUS_ACCESS_DENIED;
            break;
        default:
            status = SMB2_STATUS_INTERNAL_ERROR;
            break;
    } /* switch */

    chimera_smb_complete_request(request, status);
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
chimera_smb_parse_sd_to_acl(
    const uint8_t            *sd_buf,
    uint32_t                  sd_len,
    struct chimera_vfs_attrs *attrs,
    void                     *acl_buf,
    uint32_t                  acl_buf_len)
{
    struct chimera_acl *acl     = acl_buf;
    unsigned            acl_max = (acl_buf_len - sizeof(struct chimera_acl)) /
        sizeof(struct chimera_ace);

    chimera_smb_sd_to_acl(sd_buf, sd_len, attrs, acl, acl_max);
} /* chimera_smb_parse_sd_to_acl */

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

    {
        struct chimera_acl *acl_buf =
            (struct chimera_acl *) request->set_info.acl_storage;
        unsigned            acl_max = (sizeof(request->set_info.acl_storage) -
                                       sizeof(struct chimera_acl)) /
            sizeof(struct chimera_ace);

        chimera_smb_sd_to_acl(sd_buf, sd_len, vfs_attrs, acl_buf, acl_max);
    }

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
    struct chimera_smb_request *request   = private_data;
    uint32_t                    addl_info = request->query_info.addl_info;
    int                         sd_len;

    if (error_code) {
        chimera_smb_open_file_release(request, request->query_info.open_file);
        chimera_smb_complete_request(request, SMB2_STATUS_INTERNAL_ERROR);
        return;
    }

    /* Build the security descriptor now, while the (callback-scoped) ACL is
     * valid, and stash the bytes for the reply builder. */
    sd_len = chimera_smb_acl_to_sd(
        attr->va_uid, attr->va_gid, attr->va_mode & 07777,
        (attr->va_set_mask & CHIMERA_VFS_ATTR_ACL) ? attr->va_acl : NULL,
        !!(addl_info & OWNER_SECURITY_INFORMATION),
        !!(addl_info & GROUP_SECURITY_INFORMATION),
        !!(addl_info & DACL_SECURITY_INFORMATION),
        request->query_info.sec_buf,
        sizeof(request->query_info.sec_buf));

    if (sd_len < 0) {
        chimera_smb_open_file_release(request, request->query_info.open_file);
        chimera_smb_complete_request(request, SMB2_STATUS_BUFFER_OVERFLOW);
        return;
    }

    request->query_info.sec_buf_len = (uint32_t) sd_len;

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
        CHIMERA_VFS_ATTR_MASK_STAT | CHIMERA_VFS_ATTR_ACL,
        chimera_smb_query_security_getattr_callback,
        request);
} /* chimera_smb_query_security */

void
chimera_smb_query_security_reply(
    struct evpl_iovec_cursor   *reply_cursor,
    struct chimera_smb_request *request)
{
    uint32_t sd_len = request->query_info.sec_buf_len;

    evpl_iovec_cursor_append_uint16(reply_cursor, SMB2_QUERY_INFO_REPLY_SIZE);
    evpl_iovec_cursor_append_uint16(reply_cursor, 64 + 8);
    evpl_iovec_cursor_append_uint32(reply_cursor, sd_len);
    evpl_iovec_cursor_append_blob_unaligned(reply_cursor,
                                            request->query_info.sec_buf, sd_len);
} /* chimera_smb_query_security_reply */
