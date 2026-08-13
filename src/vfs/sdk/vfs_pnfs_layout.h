// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <stdint.h>

#include "vfs_attrs.h"   /* CHIMERA_VFS_FH_SIZE */

/*
 * Backend-sourced layouts (CHIMERA_VFS_CAP_LAYOUT_SOURCE).
 *
 * A layout-producing backend answers CHIMERA_VFS_OP_GET_LAYOUT with these
 * protocol-neutral descriptors; the NFS server owns all NFSv4 XDR (flex-files
 * RFC 8435 / block RFC 5663) and merely encodes what the backend returns.  A
 * segment carries both the flex field (a data-server file handle) and the block
 * fields (a volume-relative extent); which are meaningful is set by the layout
 * class.
 */

#define CHIMERA_VFS_DEVICEID_SIZE       16   /* == NFS4_DEVICEID4_SIZE */

#define CHIMERA_VFS_LAYOUT_MAX_SEGMENTS 8
#define CHIMERA_VFS_LAYOUT_MAX_DEVICES  4

enum chimera_vfs_layout_class {
    CHIMERA_VFS_LAYOUT_CLASS_FLEX  = 1,   /* flex-files (RFC 8435)   */
    CHIMERA_VFS_LAYOUT_CLASS_BLOCK = 2,   /* block volume (RFC 5663) */
    CHIMERA_VFS_LAYOUT_CLASS_SCSI  = 3,   /* SCSI volume (RFC 8154)  */
};

/* Block-extent state, numerically the RFC 5663 pnfs_block_extent_state4. */
#define CHIMERA_VFS_BLOCK_READ_WRITE_DATA 0
#define CHIMERA_VFS_BLOCK_READ_DATA       1
#define CHIMERA_VFS_BLOCK_INVALID_DATA    2
#define CHIMERA_VFS_BLOCK_NONE_DATA       3

struct chimera_vfs_layout_segment {
    uint64_t offset;                       /* file offset of this segment            */
    uint64_t length;                       /* bytes; UINT64_MAX == to EOF            */
    uint32_t iomode;                       /* LAYOUTIOMODE4_{READ,RW}                */
    uint8_t  deviceid[CHIMERA_VFS_DEVICEID_SIZE];

    /* FLEX: the data server's native file handle for this file. */
    uint32_t ds_fh_len;
    uint8_t  ds_fh[CHIMERA_VFS_FH_SIZE];

    /* BLOCK (RFC 5663 pnfs_block_extent4): maps [offset,length) onto the volume
     * named by deviceid at blk_vol_offset, in state blk_state. */
    uint64_t blk_vol_offset;
    uint32_t blk_state;                    /* CHIMERA_VFS_BLOCK_*                     */
};

struct chimera_vfs_layout_device {
    uint8_t  deviceid[CHIMERA_VFS_DEVICEID_SIZE];
    uint32_t layout_class;                 /* enum chimera_vfs_layout_class           */

    /* FLEX: one data-server network address. */
    char     netid[8];
    char     uaddr[64];

    /* BLOCK: a single SIMPLE volume (RFC 5663) the client matches to a local
     * disk by the signature {blk_sig_offset, blk_sig[0..blk_sig_len)}. */
    uint64_t blk_vol_size;
    uint64_t blk_sig_offset;
    uint32_t blk_sig_len;
    uint8_t  blk_sig[64];

    /* SCSI (RFC 8154): a single BASE volume the client matches to a local LU
     * by its SCSI VPD-0x83 designator (a hardware identifier; nothing is
     * written to the disk).  scsi_pr_key is the persistent-reservation key the
     * client registers with -- the server does not issue reservations in v1. */
    uint32_t scsi_code_set;                /* PS_CODE_SET_BINARY=1 / _ASCII=2         */
    uint32_t scsi_desig_type;              /* PS_DESIGNATOR_T10=1 / _EUI64=2 / _NAA=3 */
    uint32_t scsi_desig_len;
    uint8_t  scsi_desig[32];
    uint64_t scsi_pr_key;
};
