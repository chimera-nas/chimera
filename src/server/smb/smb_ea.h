// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <stdint.h>
#include <string.h>
#include <ctype.h>

#include "smb2.h"
#include "vfs/vfs_error.h"
#include "vfs/vfs_xattr_name.h"

/*
 * SMB2 Extended Attributes (MS-FSCC 2.4.15 FILE_FULL_EA_INFORMATION and 2.4.15.1
 * FILE_GET_EA_INFORMATION).  Pure wire-format + naming helpers shared by the
 * query / set / create paths; the async VFS plumbing lives in the proc files.
 *
 * EA names are bare OS/2 ASCII strings, case-insensitive.  Chimera maps them
 * onto the shared "user." VFS xattr keyspace (see vfs_xattr_name.h), so an SMB
 * EA "FOO" and an NFS user-xattr "FOO" are the same backend object.  Per Samba,
 * the stored case is preserved but matched case-insensitively; a zero-length
 * value on set deletes the attribute.
 */

/* FILE_FULL_EA_INFORMATION Flags field (MS-FSCC): only FILE_NEED_EA is defined.
 * We round-trip a zero Flags byte and do not enforce the legacy need-EA gate, so
 * the stored xattr value stays byte-identical across protocols. */
#ifndef FILE_NEED_EA
#define FILE_NEED_EA             0x80
#endif // ifndef FILE_NEED_EA

/* QUERY_INFO Flags for EA enumeration (MS-SMB2 2.2.37). */
#ifndef SL_RESTART_SCAN
#define SL_RESTART_SCAN          0x00000001
#define SL_RETURN_SINGLE_ENTRY   0x00000002
#define SL_INDEX_SPECIFIED       0x00000004
#endif // ifndef SL_RESTART_SCAN

/* Largest single EA value Chimera accepts (matches the Linux user-xattr cap and
 * keeps an EA comfortably inside one VFS value buffer). */
#define CHIMERA_SMB_EA_VALUE_MAX 65536

/* Map a VFS error from an xattr op to the NTSTATUS an EA query/set returns. */
static inline uint32_t
chimera_smb_ea_status(enum chimera_vfs_error err)
{
    switch (err) {
        case CHIMERA_VFS_OK:
            return SMB2_STATUS_SUCCESS;
        case CHIMERA_VFS_ENOTSUP:
            return SMB2_STATUS_EAS_NOT_SUPPORTED;
        case CHIMERA_VFS_ENODATA:
            return SMB2_STATUS_NONEXISTENT_EA_ENTRY;
        case CHIMERA_VFS_ERANGE:
        case CHIMERA_VFS_EFBIG:
            return SMB2_STATUS_EA_TOO_LARGE;
        case CHIMERA_VFS_EEXIST:
            return SMB2_STATUS_OBJECT_NAME_COLLISION;
        case CHIMERA_VFS_ENOENT:
            return SMB2_STATUS_OBJECT_NAME_NOT_FOUND;
        default:
            return SMB2_STATUS_INTERNAL_ERROR;
    } /* switch */
} /* chimera_smb_ea_status */

/* Case-insensitive ASCII comparison of two (client-facing) EA names. */
static inline int
chimera_smb_ea_name_eq(
    const char *a,
    uint32_t    alen,
    const char *b,
    uint32_t    blen)
{
    uint32_t i;

    if (alen != blen) {
        return 0;
    }
    for (i = 0; i < alen; i++) {
        if (toupper((unsigned char) a[i]) != toupper((unsigned char) b[i])) {
            return 0;
        }
    }
    return 1;
} /* chimera_smb_ea_name_eq */

/*
 * Validate a client-facing EA name (MS-FSCC 2.4.15 / MS-FSA 2.1.5.14.2).  An EA
 * name is invalid if it contains a control character (< 0x20) or any of the
 * reserved punctuation set -- matching Samba's is_invalid_windows_ea_name.  An
 * invalid name fails the SET with STATUS_INVALID_EA_NAME.  (An empty name is
 * rejected separately by the caller.)
 */
static inline int
chimera_smb_ea_name_valid(
    const char *name,
    uint32_t    len)
{
    uint32_t i;

    for (i = 0; i < len; i++) {
        unsigned char c = (unsigned char) name[i];

        if (c < 0x20 || strchr("\"*+,/:;<=>?[\\]|", (char) c)) {
            return 0;
        }
    }
    return 1;
} /* chimera_smb_ea_name_valid */

/* One decoded FILE_FULL_EA_INFORMATION entry; name/value point into the buffer. */
struct chimera_smb_ea_entry {
    uint8_t     flags;
    const char *name;       /* client-facing (no "user.") ASCII name */
    uint32_t    name_len;
    const void *value;
    uint32_t    value_len;
};

/*
 * Decode the FILE_FULL_EA_INFORMATION entry at buf[*off], advancing *off to the
 * next entry (or to len when this was the last).  Returns 0 on success, -1 on a
 * malformed entry.  Validates lengths against the buffer bound.
 */
static inline int
chimera_smb_ea_full_parse_one(
    const uint8_t               *buf,
    uint32_t                     len,
    uint32_t                    *off,
    struct chimera_smb_ea_entry *e)
{
    uint32_t pos = *off;
    uint32_t next_off, hdr, need;

    if (pos + 8 > len) {
        return -1;
    }

    memcpy(&next_off, buf + pos, 4);
    e->flags     = buf[pos + 4];
    e->name_len  = buf[pos + 5];
    e->value_len = (uint16_t) (buf[pos + 6] | (buf[pos + 7] << 8));

    hdr = 8;
    /* name + NUL + value must fit within this entry. */
    need = hdr + e->name_len + 1 + e->value_len;
    if (pos + need > len) {
        return -1;
    }

    e->name  = (const char *) (buf + pos + hdr);
    e->value = buf + pos + hdr + e->name_len + 1;

    if (next_off == 0) {
        *off = len;            /* last entry */
    } else {
        if (next_off < need || pos + next_off > len) {
            return -1;
        }
        *off = pos + next_off;
    }
    return 0;
} /* chimera_smb_ea_full_parse_one */

/*
 * Decode a FILE_GET_EA_INFORMATION entry (NextEntryOffset, EaNameLength, EaName,
 * NUL) at buf[*off] -- the optional name list a FULL_EA query may carry to
 * select specific EAs.  Returns 0 on success, -1 on malformed.
 */
static inline int
chimera_smb_ea_get_parse_one(
    const uint8_t *buf,
    uint32_t       len,
    uint32_t      *off,
    const char   **name,
    uint32_t      *name_len)
{
    uint32_t pos = *off;
    uint32_t next_off, need;

    if (pos + 5 > len) {
        return -1;
    }
    memcpy(&next_off, buf + pos, 4);
    *name_len = buf[pos + 4];
    need      = 5 + *name_len + 1;
    if (pos + need > len) {
        return -1;
    }
    *name = (const char *) (buf + pos + 5);

    if (next_off == 0) {
        *off = len;
    } else {
        if (next_off < need || pos + next_off > len) {
            return -1;
        }
        *off = pos + next_off;
    }
    return 0;
} /* chimera_smb_ea_get_parse_one */

/*
 * Encode one FILE_FULL_EA_INFORMATION entry into dst (caller guarantees room),
 * with the given client-facing name and value.  is_last controls NextEntryOffset
 * (0 if last, else the 4-byte-aligned entry size).  Returns the aligned number
 * of bytes consumed.  dst==NULL measures only.
 */
static inline uint32_t
chimera_smb_ea_full_emit_one(
    uint8_t    *dst,
    const char *name,
    uint8_t     name_len,
    const void *value,
    uint16_t    value_len,
    int         is_last)
{
    uint32_t entry_size = 8 + name_len + 1 + value_len;
    uint32_t aligned    = is_last ? entry_size : ((entry_size + 3) & ~3u);
    uint32_t next       = is_last ? 0 : aligned;

    if (dst) {
        memcpy(dst, &next, 4);
        dst[4] = 0;                 /* Flags: we do not surface FILE_NEED_EA */
        dst[5] = name_len;
        dst[6] = (uint8_t) (value_len & 0xff);
        dst[7] = (uint8_t) (value_len >> 8);
        memcpy(dst + 8, name, name_len);
        dst[8 + name_len] = '\0';
        if (value_len) {
            memcpy(dst + 8 + name_len + 1, value, value_len);
        }
        /* zero the alignment tail */
        for (uint32_t p = entry_size; p < aligned; p++) {
            dst[p] = 0;
        }
    }
    return aligned;
} /* chimera_smb_ea_full_emit_one */
